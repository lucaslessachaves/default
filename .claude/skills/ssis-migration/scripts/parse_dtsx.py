#!/usr/bin/env python3
"""
DTSX Parser — Extracts structured metadata from SSIS package files.

Usage:
    python parse_dtsx.py <path_to_dtsx> [--output json|markdown]

Output:
    Structured summary of package components: connections, variables,
    control flow tasks, data flow components, and precedence constraints.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

# DTS namespace
NS = {"DTS": "www.microsoft.com/SqlServer/Dts"}


def parse_connection_managers(root):
    """Extract all connection managers."""
    connections = []
    for cm in root.findall(".//DTS:ConnectionManager", NS):
        name = cm.get(f"{{{NS['DTS']}}}ObjectName", "Unknown")
        creation = cm.get(f"{{{NS['DTS']}}}CreationName", "Unknown")
        conn_str = ""
        inner = cm.find(".//DTS:ConnectionManager", NS)
        if inner is not None:
            conn_str = inner.get(f"{{{NS['DTS']}}}ConnectionString", "")
        connections.append({
            "name": name,
            "type": creation,
            "connection_string": conn_str
        })
    return connections


def parse_variables(root):
    """Extract all package variables."""
    variables = []
    for var in root.findall(".//DTS:Variable", NS):
        ns = var.get(f"{{{NS['DTS']}}}Namespace", "User")
        name = var.get(f"{{{NS['DTS']}}}ObjectName", "Unknown")
        val_elem = var.find("DTS:VariableValue", NS)
        data_type = val_elem.get(f"{{{NS['DTS']}}}DataType", "") if val_elem is not None else ""
        value = val_elem.text if val_elem is not None and val_elem.text else ""
        variables.append({
            "namespace": ns,
            "name": name,
            "data_type": data_type,
            "default_value": value
        })
    return variables


def parse_executables(root, depth=0):
    """Recursively extract all executable tasks."""
    tasks = []
    for exe in root.findall("DTS:Executables/DTS:Executable", NS):
        name = exe.get(f"{{{NS['DTS']}}}ObjectName", "Unknown")
        exe_type = exe.get(f"{{{NS['DTS']}}}CreationName", "Unknown")

        task = {
            "name": name,
            "type": exe_type,
            "depth": depth
        }

        # Extract SQL for Execute SQL Tasks
        sql_data = exe.find(".//{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlTaskData")
        if sql_data is None:
            # Try without namespace
            for elem in exe.iter():
                tag = elem.tag if isinstance(elem.tag, str) else ""
                if "SqlTaskData" in tag:
                    sql_data = elem
                    break

        if sql_data is not None:
            for attr in sql_data.attrib:
                if "SqlStatementSource" in attr:
                    task["sql"] = sql_data.get(attr, "")

        # Extract Data Flow components
        pipeline = exe.find(".//pipeline")
        if pipeline is not None:
            components = []
            for comp in pipeline.findall(".//component"):
                comp_name = comp.get("name", "Unknown")
                comp_class = comp.get("componentClassID", "Unknown")
                comp_info = {"name": comp_name, "class": comp_class}

                # Extract SQL from properties
                for prop in comp.findall(".//property"):
                    if prop.get("name") == "SqlCommand" and prop.text:
                        comp_info["sql"] = prop.text.strip()

                # Extract output columns
                columns = []
                for col in comp.findall(".//outputColumn"):
                    columns.append({
                        "name": col.get("name", ""),
                        "dataType": col.get("dataType", ""),
                        "expression": col.get("expression", "")
                    })
                if columns:
                    comp_info["output_columns"] = columns

                components.append(comp_info)
            task["data_flow_components"] = components

        # Recurse into containers
        children = parse_executables(exe, depth + 1)
        if children:
            task["children"] = children

        tasks.append(task)
    return tasks


def parse_precedence_constraints(root):
    """Extract execution order constraints."""
    constraints = []
    for pc in root.findall(".//DTS:PrecedenceConstraint", NS):
        from_task = pc.get(f"{{{NS['DTS']}}}From", "")
        to_task = pc.get(f"{{{NS['DTS']}}}To", "")
        value = pc.get(f"{{{NS['DTS']}}}Value", "0")
        eval_op = pc.get(f"{{{NS['DTS']}}}EvalOp", "")

        constraint_type = {
            "0": "On Success",
            "1": "On Failure",
            "2": "On Completion"
        }.get(value, f"Unknown ({value})")

        constraints.append({
            "from": from_task.split("\\")[-1] if "\\" in from_task else from_task,
            "to": to_task.split("\\")[-1] if "\\" in to_task else to_task,
            "type": constraint_type,
            "eval_op": eval_op
        })
    return constraints


def parse_dtsx(file_path: str) -> dict:
    """Parse a DTSX file and return structured metadata."""
    tree = etree.parse(file_path)
    root = tree.getroot()

    package_name = root.get(f"{{{NS['DTS']}}}ObjectName", Path(file_path).stem)

    return {
        "package_name": package_name,
        "file": str(file_path),
        "connection_managers": parse_connection_managers(root),
        "variables": parse_variables(root),
        "tasks": parse_executables(root),
        "precedence_constraints": parse_precedence_constraints(root)
    }


def to_markdown(result: dict) -> str:
    """Convert parsed result to Markdown summary."""
    lines = [f"# Package: {result['package_name']}\n"]

    lines.append("## Connection Managers\n")
    for cm in result["connection_managers"]:
        lines.append(f"- **{cm['name']}** ({cm['type']})")

    lines.append("\n## Variables\n")
    for v in result["variables"]:
        lines.append(f"- `{v['namespace']}::{v['name']}` (type={v['data_type']}, default={v['default_value']})")

    lines.append("\n## Control Flow Tasks\n")
    for task in result["tasks"]:
        indent = "  " * task["depth"]
        lines.append(f"{indent}- **{task['name']}** [{task['type']}]")
        if "sql" in task:
            lines.append(f"{indent}  ```sql\n{indent}  {task['sql']}\n{indent}  ```")
        if "data_flow_components" in task:
            for comp in task["data_flow_components"]:
                lines.append(f"{indent}  - {comp['name']} ({comp['class']})")
                if "sql" in comp:
                    lines.append(f"{indent}    ```sql\n{indent}    {comp['sql']}\n{indent}    ```")

    lines.append("\n## Execution Order\n")
    for pc in result["precedence_constraints"]:
        lines.append(f"- {pc['from']} → {pc['to']} [{pc['type']}]")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Parse SSIS DTSX package files")
    parser.add_argument("file", help="Path to .dtsx file")
    parser.add_argument("--output", choices=["json", "markdown"], default="markdown")
    args = parser.parse_args()

    if not Path(args.file).exists():
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    result = parse_dtsx(args.file)

    if args.output == "json":
        print(json.dumps(result, indent=2))
    else:
        print(to_markdown(result))


if __name__ == "__main__":
    main()
