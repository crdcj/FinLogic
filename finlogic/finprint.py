from rich.console import Console
from rich.table import Table

console = Console()


def print_dict(info_dict: dict, table_name) -> None:
    """Print a dictionary in a table format with Rich."""
    if info_dict is not None:
        table = Table(
            title=table_name,
            # title_style="bold blue",
            header_style="bold white on cyan",
            # row_styles=["none", "dim"],
            show_lines=True,
        )

        # Add columns
        table.add_column("Item", style="bold")
        table.add_column("Value", justify="right")

        # Add rows
        for key, value in info_dict.items():
            if isinstance(value, int):
                formatted_value = f"{value:,}"
            else:
                formatted_value = str(value)
            table.add_row(key, formatted_value)

        # Print the table
        console.print(table)
    else:
        console.print("[bold red]Dictionary is empty[/bold red]")
