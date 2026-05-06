"""
Query statistics tracking for adaptive indexing
"""
import json
import os


STATS_FILE = "index/query_stats.json"


class QueryStats:
    """Central repository for query statistics tracking"""
    
    def __init__(self, database=None):
        """Initialize query stats, load from disk if available"""
        self.stats = {}
        self.database = database
        self._load_from_disk()

    def _stats_file_path(self):
        if self.database:
            return os.path.join("index", self.database, "query_stats.json")
        return STATS_FILE
    
    def _load_from_disk(self):
        """Load query stats from disk"""
        stats_file = self._stats_file_path()
        if os.path.exists(stats_file):
            try:
                with open(stats_file, 'r') as f:
                    self.stats = json.load(f)
            except Exception as e:
                print(f"Error loading query stats: {e}")
                self.stats = {}
        else:
            self.stats = {}
    
    def _save_to_disk(self):
        """Save query stats to disk"""
        try:
            stats_file = self._stats_file_path()
            os.makedirs(os.path.dirname(stats_file), exist_ok=True)
            with open(stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            print(f"Error saving query stats: {e}")
    
    def record_equality_query(self, table, column):
        """
        Record an equality query on a column.
        
        Args:
            table: Table name
            column: Column name
        """
        if table not in self.stats:
            self.stats[table] = {}
        
        if column not in self.stats[table]:
            self.stats[table][column] = {
                "equality_count": 0,
                "range_count": 0
            }
        
        self.stats[table][column]["equality_count"] += 1
        self._save_to_disk()
    
    def record_range_query(self, table, column):
        """
        Record a range query on a column.
        
        Args:
            table: Table name
            column: Column name
        """
        if table not in self.stats:
            self.stats[table] = {}
        
        if column not in self.stats[table]:
            self.stats[table][column] = {
                "equality_count": 0,
                "range_count": 0
            }
        
        self.stats[table][column]["range_count"] += 1
        self._save_to_disk()
    
    def get_stats(self, table, column):
        """
        Get statistics for a table column.
        
        Args:
            table: Table name
            column: Column name
        
        Returns:
            Dict with equality_count and range_count, or None if not found
        """
        if table in self.stats and column in self.stats[table]:
            return self.stats[table][column]
        return None
    
    def should_create_hash_index(self, table, column, threshold=3):
        """
        Check if we should create a hash index based on statistics.
        
        Args:
            table: Table name
            column: Column name
            threshold: Number of queries needed to trigger index creation
        
        Returns:
            True if equality_count >= threshold
        """
        stats = self.get_stats(table, column)
        if stats is None:
            return False
        return stats["equality_count"] >= threshold
    
    def should_create_sorted_index(self, table, column, threshold=3):
        """
        Check if we should create a sorted index based on statistics.
        
        Args:
            table: Table name
            column: Column name
            threshold: Number of queries needed to trigger index creation
        
        Returns:
            True if range_count >= threshold
        """
        stats = self.get_stats(table, column)
        if stats is None:
            return False
        return stats["range_count"] >= threshold
    
    def get_all_stats(self):
        """Get all statistics"""
        return self.stats
    
    def reset_stats(self, table=None, column=None):
        """
        Reset statistics.
        
        Args:
            table: If provided, reset only this table. If None, reset all.
            column: If provided with table, reset only this column.
        """
        if table is None:
            self.stats = {}
        elif column is None:
            if table in self.stats:
                del self.stats[table]
        else:
            if table in self.stats and column in self.stats[table]:
                del self.stats[table][column]
        
        self._save_to_disk()


# Global instance
_global_stats = {}


def get_query_stats(database=None):
    """Get the QueryStats instance for a database"""
    key = database or "__default__"
    if key not in _global_stats:
        _global_stats[key] = QueryStats(database)
    return _global_stats[key]
