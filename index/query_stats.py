"""
Query statistics tracking for adaptive indexing
"""
import json
import os


STATS_FILE = "index/query_stats.json"


class QueryStats:
    """Central repository for query statistics tracking"""
    
    def __init__(self):
        """Initialize query stats, load from disk if available"""
        self.stats = {}
        self._load_from_disk()
    
    def _load_from_disk(self):
        """Load query stats from disk"""
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE, 'r') as f:
                    self.stats = json.load(f)
            except Exception as e:
                print(f"Error loading query stats: {e}")
                self.stats = {}
        else:
            self.stats = {}
    
    def _save_to_disk(self):
        """Save query stats to disk"""
        try:
            os.makedirs("index", exist_ok=True)
            with open(STATS_FILE, 'w') as f:
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
_global_stats = None


def get_query_stats():
    """Get the global QueryStats instance"""
    global _global_stats
    if _global_stats is None:
        _global_stats = QueryStats()
    return _global_stats
