# Parser module - Modular SQL query parsing
from .create_parser import parse_create
from .insert_parser import parse_insert
from .select_parser import parse_select
from .update_parser import parse_update
from .delete_parser import parse_delete
from .drop_parser import parse_drop
from .alter_parser import parse_alter
from .show_parser import parse_show
from .describe_parser import parse_describe
from .truncate_parser import parse_truncate

# Import tokenizer for main parse_query function
import sys
import os
# Add parent directory to path to import tokenizer
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tokenizer import tokenize
from visualizer import print_trace


def parse_query(query):
    """
    Main query parser - determines command type and delegates to specific parser
    """
    
    tokens = tokenize(query)
    
    if not tokens:
        raise Exception("Empty query")
    
    command_type = tokens[0].upper()
    
    # Route to appropriate parser based on command type
    command = None
    
    if command_type == "CREATE":
        command = parse_create(tokens)
    
    elif command_type == "INSERT":
        command = parse_insert(tokens)
    
    elif command_type == "SELECT":
        command = parse_select(tokens)
    
    elif command_type == "UPDATE":
        command = parse_update(tokens)
    
    elif command_type == "DELETE":
        command = parse_delete(tokens)
    
    elif command_type == "DROP":
        command = parse_drop(tokens)
    
    elif command_type == "ALTER":
        command = parse_alter(tokens)
    
    elif command_type == "SHOW":
        command = parse_show(tokens)
    
    elif command_type == "DESCRIBE":
        command = parse_describe(tokens)
    
    elif command_type == "TRUNCATE":
        command = parse_truncate(tokens)
    
    else:
        raise Exception(f"Unsupported command: {command_type}")
    
    # Display parsed command structure
    _display_parsed_command(command)
    
    return command


def _display_parsed_command(command):
    """Display the parsed command structure in educational mode"""
    import config
    
    if config.get_mode() != "EDUCATIONAL":
        return
    
    lines = [f"Operation Type : {command['type']}"]
    
    # Display relevant fields based on command type
    if 'table' in command:
        lines.append(f"Target Table   : {command['table']}")
    
    if 'columns' in command and command['columns']:
        if command['type'] == 'CREATE':
            cols = [f"{name}({dtype})" for name, dtype in command['columns']]
            lines.append(f"Columns        : {cols}")
        else:
            lines.append(f"Target Columns : {command['columns']}")
    
    if 'values' in command:
        lines.append(f"Values         : {command['values']}")
    
    if 'condition' in command and command['condition']:
        cond = command['condition']
        if isinstance(cond, tuple):
            # Tuple format: (column, operator, value)
            lines.append(f"Condition      : {cond[0]} {cond[1]} {cond[2]}")
        elif isinstance(cond, dict):
            # Dict format: {'column': ..., 'operator': ..., 'value': ...}
            lines.append(f"Condition      : {cond['column']} {cond['operator']} {cond['value']}")
        else:
            lines.append(f"Condition      : {cond}")
    
    if 'set' in command:
        set_data = command['set']
        if isinstance(set_data, tuple):
            # Tuple format: (column, value)
            lines.append(f"Set Values     : {set_data[0]} = {set_data[1]}")
        elif isinstance(set_data, dict):
            # Dict format: {'column': value, ...}
            updates = [f"{k}={v}" for k, v in set_data.items()]
            lines.append(f"Set Values     : {', '.join(updates)}")
        else:
            lines.append(f"Set Values     : {set_data}")
    
    if 'primary_key' in command and command['primary_key']:
        pk = command['primary_key']
        if isinstance(pk, list):
            lines.append(f"Primary Key    : ({', '.join(pk)})")
        else:
            lines.append(f"Primary Key    : {pk}")
    
    if 'operation' in command:
        lines.append(f"ALTER Operation: {command['operation']}")
    
    if 'aggregate' in command and command['aggregate']:
        agg_display = command['aggregate']
        if 'agg_column' in command and command['agg_column']:
            agg_display = f"{command['aggregate']}({command['agg_column']})"
        lines.append(f"Aggregate Func : {agg_display}")
    
    if 'group_by' in command and command['group_by']:
        lines.append(f"Group By       : {command['group_by']}")
    
    if 'order_by' in command and command['order_by']:
        order = command['order_by']
        if isinstance(order, tuple):
            # Tuple format: (column, direction)
            lines.append(f"Order By       : {order[0]} {order[1]}")
        elif isinstance(order, dict):
            # Dict format: {'column': ..., 'direction': ...}
            lines.append(f"Order By       : {order['column']} {order['direction']}")
        else:
            lines.append(f"Order By       : {order}")
    
    if 'limit' in command and command['limit']:
        lines.append(f"Limit          : {command['limit']}")
    
    print_trace("PARSER", lines)


__all__ = [
    'parse_query',
    'parse_create',
    'parse_insert',
    'parse_select',
    'parse_update',
    'parse_delete',
    'parse_drop',
    'parse_alter',
    'parse_show',
    'parse_describe',
    'parse_truncate'
]
