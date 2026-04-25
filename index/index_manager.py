"""
Index Manager for MiniDB
Manages index creation, loading, and usage
"""
import json
from .hash_index import HashIndex
from .sorted_index import SortedIndex
from .query_stats import get_query_stats
from .index_utils import (
    index_exists,
    delete_index,
    list_indices,
    ensure_index_dir
)


class IndexManager:
    """
    Central manager for all index operations.
    Handles creation, loading, caching, and usage of indices.
    """
    
    def __init__(self):
        """Initialize index manager"""
        self.indices = {}  # {(table, column, type): index_object}
        self.stats = get_query_stats()
        ensure_index_dir()
    
    def _cache_key(self, table, column, index_type):
        """Generate cache key for an index"""
        return (table, column, index_type.lower())
    
    def create_hash_index(self, table, column, table_data, column_index):
        """
        Create and cache a hash index.
        
        Args:
            table: Table name
            column: Column name
            table_data: List of rows [row1, row2, ...]
            column_index: Index of column in each row
        
        Returns:
            HashIndex object
        """
        index = HashIndex(table, column)
        index.build_from_table(table_data, column_index)
        index.save()
        
        key = self._cache_key(table, column, 'hash')
        self.indices[key] = index
        
        return index
    
    def create_sorted_index(self, table, column, table_data, column_index):
        """
        Create and cache a sorted index.
        
        Args:
            table: Table name
            column: Column name
            table_data: List of rows [row1, row2, ...]
            column_index: Index of column in each row
        
        Returns:
            SortedIndex object
        """
        index = SortedIndex(table, column)
        index.build_from_table(table_data, column_index)
        index.save()
        
        key = self._cache_key(table, column, 'sorted')
        self.indices[key] = index
        
        return index
    
    def get_hash_index(self, table, column):
        """
        Get or load a hash index.
        
        Args:
            table: Table name
            column: Column name
        
        Returns:
            HashIndex object if exists, None otherwise
        """
        key = self._cache_key(table, column, 'hash')
        
        # Check cache
        if key in self.indices:
            return self.indices[key]
        
        # Try to load from disk
        if index_exists(table, column, 'hash'):
            index = HashIndex(table, column)
            if index.load():
                self.indices[key] = index
                return index
        
        return None
    
    def get_sorted_index(self, table, column):
        """
        Get or load a sorted index.
        
        Args:
            table: Table name
            column: Column name
        
        Returns:
            SortedIndex object if exists, None otherwise
        """
        key = self._cache_key(table, column, 'sorted')
        
        # Check cache
        if key in self.indices:
            return self.indices[key]
        
        # Try to load from disk
        if index_exists(table, column, 'sorted'):
            index = SortedIndex(table, column)
            if index.load():
                self.indices[key] = index
                return index
        
        return None
    
    def delete_hash_index(self, table, column):
        """Delete a hash index"""
        key = self._cache_key(table, column, 'hash')
        if key in self.indices:
            del self.indices[key]
        return delete_index(table, column, 'hash')
    
    def delete_sorted_index(self, table, column):
        """Delete a sorted index"""
        key = self._cache_key(table, column, 'sorted')
        if key in self.indices:
            del self.indices[key]
        return delete_index(table, column, 'sorted')
    
    def list_indices(self, table):
        """List all indices for a table"""
        return list_indices(table)
    
    def search_with_index(self, table, column, operator, value):
        """
        Search using appropriate index if available.
        
        Args:
            table: Table name
            column: Column name
            operator: '=', '<', '>', '<=', '>=', 'BETWEEN'
            value: Value(s) for comparison
        
        Returns:
            List of row numbers, or None if no suitable index
        """
        if operator == '=':
            # Try hash index first for equality
            index = self.get_hash_index(table, column)
            if index:
                return index.search(value)
        
        if operator in ['<', '>', '<=', '>=', 'BETWEEN']:
            # Use sorted index for range queries
            index = self.get_sorted_index(table, column)
            if index:
                if operator == '=':
                    return index.search_equal(value)
                elif operator == '>':
                    return index.search_greater_than(value, include_equal=False)
                elif operator == '>=':
                    return index.search_greater_than(value, include_equal=True)
                elif operator == '<':
                    return index.search_less_than(value, include_equal=False)
                elif operator == '<=':
                    return index.search_less_than(value, include_equal=True)
                elif operator == 'BETWEEN':
                    lower, upper = value
                    return index.search_between(lower, upper)
        
        return None
    
    def record_query(self, table, column, operator):
        """
        Record a query and check if index should be created.
        
        Args:
            table: Table name
            column: Column name
            operator: Query operator ('=', '<', '>', '<=', '>=', 'BETWEEN')
        
        Returns:
            Tuple (should_create_hash, should_create_sorted)
        """
        if operator == '=':
            self.stats.record_equality_query(table, column)
        elif operator in ['<', '>', '<=', '>=', 'BETWEEN']:
            self.stats.record_range_query(table, column)
        
        should_create_hash = self.stats.should_create_hash_index(table, column)
        should_create_sorted = self.stats.should_create_sorted_index(table, column)
        
        # Check if we already created these indices
        if should_create_hash and not index_exists(table, column, 'hash'):
            return (True, should_create_sorted)
        
        if should_create_sorted and not index_exists(table, column, 'sorted'):
            return (should_create_hash, True)
        
        return (False, False)
    
    def rebuild_index(self, table, column, columns_info, table_data):
        """
        Rebuild an index from scratch.
        
        Args:
            table: Table name
            column: Column name
            columns_info: List of [name, type] for all columns
            table_data: List of rows
        
        Returns:
            True if rebuilt, False if unsuccessful
        """
        # Find column index
        column_index = None
        for i, (col_name, _) in enumerate(columns_info):
            if col_name == column:
                column_index = i
                break
        
        if column_index is None:
            return False
        
        # Determine which index to create based on stats
        stats = self.stats.get_stats(table, column)
        
        if stats and stats['equality_count'] >= stats['range_count']:
            # Create hash index
            self.create_hash_index(table, column, table_data, column_index)
            return True
        elif stats:
            # Create sorted index
            self.create_sorted_index(table, column, table_data, column_index)
            return True
        
        return False
    
    def clear_cache(self):
        """Clear all cached indices"""
        self.indices.clear()
    
    def get_stats_info(self, table, column):
        """Get statistics for a table column"""
        return self.stats.get_stats(table, column)


# Global instance
_global_manager = None


def get_index_manager():
    """Get the global IndexManager instance"""
    global _global_manager
    if _global_manager is None:
        _global_manager = IndexManager()
    return _global_manager
