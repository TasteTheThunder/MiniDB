"""
Sorted Index implementation for MiniDB
Used for range queries with binary search support
"""
import bisect
from .index_utils import save_sorted_index, load_index_data


class SortedIndex:
    """
    Sorted index for range queries.
    
    Structure: [(value, row_number), ...] sorted by value
    Uses binary search for efficient range queries
    """
    
    def __init__(self, table, column):
        """
        Initialize sorted index.
        
        Args:
            table: Table name
            column: Column name
        """
        self.table = table
        self.column = column
        self.data = []  # [(value, row_num), ...] sorted by value
    
    def build_from_table(self, table_data, column_index):
        """
        Build sorted index from table data.
        
        Args:
            table_data: List of row dicts or list of lists
            column_index: Index of the column in each row
        """
        self.data = []
        
        for row_num, row in enumerate(table_data):
            # Handle both dict and list formats
            if isinstance(row, dict):
                value = row.get(self.column, "")
            else:
                value = row[column_index] if column_index < len(row) else ""
            
            value_str = str(value).strip()
            self.data.append((value_str, row_num))
        
        # Sort by value for efficient range queries
        self._sort_data()
    
    def _sort_data(self):
        """Sort index data by value"""
        self.data.sort(key=lambda x: self._sort_key(x[0]))
    
    @staticmethod
    def _sort_key(value):
        """
        Generate sort key for value.
        Tries numeric sort first, falls back to string sort.
        """
        try:
            return (0, float(value))
        except (ValueError, TypeError):
            return (1, str(value))
    
    def _binary_search_left(self, value):
        """
        Binary search for leftmost position where value could be inserted.
        
        Args:
            value: The value to search for
        
        Returns:
            Index in self.data
        """
        key = self._sort_key(value)
        left, right = 0, len(self.data)
        
        while left < right:
            mid = (left + right) // 2
            if self._sort_key(self.data[mid][0]) < key:
                left = mid + 1
            else:
                right = mid
        
        return left
    
    def _binary_search_right(self, value):
        """
        Binary search for rightmost position where value could be inserted.
        
        Args:
            value: The value to search for
        
        Returns:
            Index in self.data
        """
        key = self._sort_key(value)
        left, right = 0, len(self.data)
        
        while left < right:
            mid = (left + right) // 2
            if key < self._sort_key(self.data[mid][0]):
                right = mid
            else:
                left = mid + 1
        
        return left
    
    def search_equal(self, value):
        """
        Search for all rows with exact value.
        
        Args:
            value: The value to search for
        
        Returns:
            List of row numbers
        """
        left = self._binary_search_left(value)
        right = self._binary_search_right(value)
        
        if left < right:
            return [row_num for _, row_num in self.data[left:right]]
        return []
    
    def search_greater_than(self, value, include_equal=False):
        """
        Find all rows where column > value (or >= if include_equal).
        
        Args:
            value: The comparison value
            include_equal: If True, use >= instead of >
        
        Returns:
            List of row numbers
        """
        if include_equal:
            idx = self._binary_search_left(value)
        else:
            idx = self._binary_search_right(value)
        
        return [row_num for _, row_num in self.data[idx:]]
    
    def search_less_than(self, value, include_equal=False):
        """
        Find all rows where column < value (or <= if include_equal).
        
        Args:
            value: The comparison value
            include_equal: If True, use <= instead of <
        
        Returns:
            List of row numbers
        """
        if include_equal:
            idx = self._binary_search_right(value)
        else:
            idx = self._binary_search_left(value)
        
        return [row_num for _, row_num in self.data[:idx]]
    
    def search_between(self, lower, upper, include_lower=True, include_upper=True):
        """
        Find all rows where lower <= column <= upper.
        
        Args:
            lower: Lower bound value
            upper: Upper bound value
            include_lower: If True, use >=, else use >
            include_upper: If True, use <=, else use <
        
        Returns:
            List of row numbers
        """
        if include_lower:
            left_idx = self._binary_search_left(lower)
        else:
            left_idx = self._binary_search_right(lower)
        
        if include_upper:
            right_idx = self._binary_search_right(upper)
        else:
            right_idx = self._binary_search_left(upper)
        
        if left_idx < right_idx:
            return [row_num for _, row_num in self.data[left_idx:right_idx]]
        return []
    
    def insert(self, value, row_num):
        """
        Insert a value-row mapping into the index.
        
        Args:
            value: The column value
            row_num: The row number
        """
        value_str = str(value).strip()
        self.data.append((value_str, row_num))
        self._sort_data()
    
    def delete(self, value, row_num):
        """
        Remove a value-row mapping from the index.
        
        Args:
            value: The column value
            row_num: The row number
        """
        value_str = str(value).strip()
        self.data = [(v, r) for v, r in self.data if not (v == value_str and r == row_num)]
    
    def save(self):
        """Save index to disk"""
        save_sorted_index(self.table, self.column, self.data)
    
    def load(self):
        """Load index from disk"""
        data = load_index_data(self.table, self.column, 'sorted')
        if data:
            self.data = data
            self._sort_data()
            return True
        return False
    
    def get_size(self):
        """Get number of entries in the index"""
        return len(self.data)
    
    def get_stats(self):
        """Get statistics about the index"""
        return {
            "type": "sorted",
            "entries": len(self.data),
            "table": self.table,
            "column": self.column
        }
