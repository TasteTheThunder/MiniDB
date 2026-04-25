"""
Index Persistence Module
Handles saving and loading of indices to/from disk
"""
import json
import os
from .index_utils import (
    get_index_path,
    ensure_index_dir,
    load_index_data,
    save_hash_index,
    save_sorted_index
)


class IndexPersistence:
    """Handles all persistence operations for indices"""
    
    @staticmethod
    def save_index(index_obj):
        """
        Save an index object to disk.
        
        Args:
            index_obj: HashIndex or SortedIndex object
        """
        if index_obj.__class__.__name__ == 'HashIndex':
            save_hash_index(index_obj.table, index_obj.column, index_obj.data)
        elif index_obj.__class__.__name__ == 'SortedIndex':
            save_sorted_index(index_obj.table, index_obj.column, index_obj.data)
    
    @staticmethod
    def load_hash_index_data(table, column):
        """
        Load hash index data from disk.
        
        Args:
            table: Table name
            column: Column name
        
        Returns:
            Dict or None
        """
        return load_index_data(table, column, 'hash')
    
    @staticmethod
    def load_sorted_index_data(table, column):
        """
        Load sorted index data from disk.
        
        Args:
            table: Table name
            column: Column name
        
        Returns:
            List or None
        """
        return load_index_data(table, column, 'sorted')
    
    @staticmethod
    def backup_index(table, column, index_type):
        """
        Create a backup of an index file.
        
        Args:
            table: Table name
            column: Column name
            index_type: 'hash' or 'sorted'
        
        Returns:
            True if backup successful, False otherwise
        """
        source_path = get_index_path(table, column, index_type)
        backup_path = source_path + ".backup"
        
        if not os.path.exists(source_path):
            return False
        
        try:
            with open(source_path, 'r') as src:
                with open(backup_path, 'w') as dst:
                    dst.write(src.read())
            return True
        except Exception as e:
            print(f"Error backing up index {source_path}: {e}")
            return False
    
    @staticmethod
    def restore_index_from_backup(table, column, index_type):
        """
        Restore an index from backup.
        
        Args:
            table: Table name
            column: Column name
            index_type: 'hash' or 'sorted'
        
        Returns:
            True if restore successful, False otherwise
        """
        source_path = get_index_path(table, column, index_type)
        backup_path = source_path + ".backup"
        
        if not os.path.exists(backup_path):
            return False
        
        try:
            with open(backup_path, 'r') as src:
                with open(source_path, 'w') as dst:
                    dst.write(src.read())
            return True
        except Exception as e:
            print(f"Error restoring index {source_path}: {e}")
            return False
    
    @staticmethod
    def get_index_file_size(table, column, index_type):
        """
        Get the size of an index file in bytes.
        
        Args:
            table: Table name
            column: Column name
            index_type: 'hash' or 'sorted'
        
        Returns:
            File size in bytes, or 0 if file doesn't exist
        """
        path = get_index_path(table, column, index_type)
        if os.path.exists(path):
            return os.path.getsize(path)
        return 0
    
    @staticmethod
    def optimize_index_file(table, column, index_type):
        """
        Optimize an index file by rewriting it.
        Useful for removing deleted entries.
        
        Args:
            table: Table name
            column: Column name
            index_type: 'hash' or 'sorted'
        
        Returns:
            True if optimization successful
        """
        from .index_manager import get_index_manager
        
        manager = get_index_manager()
        
        if index_type == 'hash':
            index = manager.get_hash_index(table, column)
            if index:
                index.save()
                return True
        elif index_type == 'sorted':
            index = manager.get_sorted_index(table, column)
            if index:
                index.save()
                return True
        
        return False
    
    @staticmethod
    def export_index_metadata(table, column, index_type):
        """
        Export metadata about an index.
        
        Args:
            table: Table name
            column: Column name
            index_type: 'hash' or 'sorted'
        
        Returns:
            Dict with metadata, or None if index doesn't exist
        """
        from .index_manager import get_index_manager
        
        manager = get_index_manager()
        
        if index_type == 'hash':
            index = manager.get_hash_index(table, column)
            if index:
                return index.get_stats()
        elif index_type == 'sorted':
            index = manager.get_sorted_index(table, column)
            if index:
                return index.get_stats()
        
        return None
