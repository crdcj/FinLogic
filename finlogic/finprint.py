from rich.console import Console
from rich.table import Table

console = Console()


def print_database_info(info_dict):
    # info_dict = None

    if info_dict is not None:
        table = Table(
            # title="FinLogic Database Information",
            title_style="bold blue",
            header_style="bold white on cyan",
            # row_styles=["none", "dim"],
            show_lines=True,
        )

        # Add columns
        table.add_column("Key", style="bold")
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
        console.print("[bold red]Finlogic Database is empty[/bold red]")
