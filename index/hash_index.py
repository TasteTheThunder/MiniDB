"""
Hash Index implementation for MiniDB
Used for equality queries
"""
import json
from .index_utils import save_hash_index, load_index_data


class HashIndex:
    """
    Hash index for fast equality lookups.
    
    Structure: {value: [row_numbers]}
    """
    
    def __init__(self, table, column):
        """
        Initialize hash index.
        
        Args:
            table: Table name
            column: Column name
        """
        self.table = table
        self.column = column
        self.data = {}  # {value: [row_numbers]}
    
    def build_from_table(self, table_data, column_index):
        """
        Build hash index from table data.
        
        Args:
            table_data: List of row dicts or list of lists
            column_index: Index of the column in each row
        """
        self.data = {}
        
        for row_num, row in enumerate(table_data):
            # Handle both dict and list formats
            if isinstance(row, dict):
                value = row.get(self.column, "")
            else:
                value = row[column_index] if column_index < len(row) else ""
            
            value_str = str(value).strip()
            
            if value_str not in self.data:
                self.data[value_str] = []
            
            self.data[value_str].append(row_num)
    
    def search(self, value):
        """
        Search for a value in the index.
        
        Args:
            value: The value to search for
        
        Returns:
            List of row numbers that match the value, or empty list
        """
        value_str = str(value).strip()
        return self.data.get(value_str, [])
    
    def insert(self, value, row_num):
        """
        Insert a value-row mapping into the index.
        
        Args:
            value: The column value
            row_num: The row number
        """
        value_str = str(value).strip()
        
        if value_str not in self.data:
            self.data[value_str] = []
        
        if row_num not in self.data[value_str]:
            self.data[value_str].append(row_num)
    
    def delete(self, value, row_num):
        """
        Remove a value-row mapping from the index.
        
        Args:
            value: The column value
            row_num: The row number
        """
        value_str = str(value).strip()
        
        if value_str in self.data and row_num in self.data[value_str]:
            self.data[value_str].remove(row_num)
            
            # Remove empty entries
            if not self.data[value_str]:
                del self.data[value_str]
    
    def save(self):
        """Save index to disk"""
        save_hash_index(self.table, self.column, self.data)
    
    def load(self):
        """Load index from disk"""
        data = load_index_data(self.table, self.column, 'hash')
        if data:
            self.data = data
            return True
        return False
    
    def get_size(self):
        """Get number of unique values in the index"""
        return len(self.data)
    
    def get_stats(self):
        """Get statistics about the index"""
        total_entries = sum(len(rows) for rows in self.data.values())
        return {
            "type": "hash",
            "unique_values": len(self.data),
            "total_entries": total_entries,
            "table": self.table,
            "column": self.column
        }
