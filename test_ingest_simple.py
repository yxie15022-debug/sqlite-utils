from sqlite_utils import cli, Database
from click.testing import CliRunner
import tempfile
import os

def main():
    runner = CliRunner()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, 'test.db')
        
        print('=' * 50)
        print('Test 1: Table does not exist - should error')
        print('=' * 50)
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='{"name": "Lila"}\n'
        )
        print(f'Exit code: {result.exit_code}')
        print(f'Output: {result.output}')
        assert result.exit_code == 1, f'Expected exit code 1, got {result.exit_code}'
        assert 'does not exist' in result.output
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 2: Create table and insert data')
        print('=' * 50)
        db = Database(db_path)
        db['chickens'].create({'name': str})
        print(f'Created table "chickens" with columns: {[c.name for c in db["chickens"].columns]}')
        db.close()
        
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='{"name": "Lila"}\n{"name": "Cleo"}\n'
        )
        print(f'Exit code: {result.exit_code}')
        if result.exit_code != 0:
            print(f'Output: {result.output}')
        assert result.exit_code == 0
        
        db = Database(db_path)
        rows = list(db['chickens'].rows)
        print(f'Rows in table: {rows}')
        assert len(rows) == 2
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 3: Insert data with new column')
        print('=' * 50)
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='{"name": "Suna", "age": 3}\n'
        )
        print(f'Exit code: {result.exit_code}')
        if result.exit_code != 0:
            print(f'Output: {result.output}')
        assert result.exit_code == 0
        
        db = Database(db_path)
        columns = [c.name for c in db['chickens'].columns]
        print(f'Columns now: {columns}')
        for col in db['chickens'].columns:
            print(f'  {col.name}: {col.type}')
        assert 'age' in columns
        
        rows = list(db['chickens'].rows)
        print(f'Rows: {rows}')
        assert len(rows) == 3
        
        suna_row = [r for r in rows if r['name'] == 'Suna'][0]
        print(f'Suna row: {suna_row}')
        # Note: age is stored as TEXT as per requirements
        assert suna_row['age'] == '3' or suna_row['age'] == 3
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 4: Invalid JSON with line number')
        print('=' * 50)
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='not valid json\n'
        )
        print(f'Exit code: {result.exit_code}')
        print(f'Output: {result.output}')
        assert result.exit_code == 1
        assert 'Invalid JSON at line 1' in result.output
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 5: Empty lines should be skipped, line numbers exclude empty lines')
        print('=' * 50)
        # Empty lines before, between, after - should be skipped
        # Line number should be 1, 2, 3 for the actual JSON lines
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='\n{"name": "A"}\n\n{"name": "B"}\n\nnot valid\n'
        )
        print(f'Exit code: {result.exit_code}')
        print(f'Output: {result.output}')
        assert result.exit_code == 1
        # The invalid JSON is at logical line 3 (after 2 valid JSONs, empty lines skipped)
        assert 'Invalid JSON at line 3' in result.output
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 6: Empty lines in valid input - should succeed')
        print('=' * 50)
        db = Database(db_path)
        count_before = db['chickens'].count
        db.close()
        
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='\n\n{"name": "Daisy"}\n\n{"name": "Molly"}\n\n'
        )
        print(f'Exit code: {result.exit_code}')
        if result.exit_code != 0:
            print(f'Output: {result.output}')
        assert result.exit_code == 0
        
        db = Database(db_path)
        count_after = db['chickens'].count
        print(f'Count before: {count_before}, Count after: {count_after}')
        assert count_after == count_before + 2
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 7: UTF-8 BOM on first non-empty line should be stripped')
        print('=' * 50)
        # UTF-8 BOM is \ufeff
        db = Database(db_path)
        count_before = db['chickens'].count
        db.close()
        
        # BOM at the start of first non-empty line
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='\ufeff{"name": "BomTest1"}\n{"name": "BomTest2"}\n'
        )
        print(f'Exit code: {result.exit_code}')
        if result.exit_code != 0:
            print(f'Output: {result.output}')
        assert result.exit_code == 0
        
        db = Database(db_path)
        count_after = db['chickens'].count
        print(f'Count before: {count_before}, Count after: {count_after}')
        assert count_after == count_before + 2
        
        # Check the data was inserted correctly
        bom_test1 = list(db['chickens'].rows_where('name = ?', ['BomTest1']))
        bom_test2 = list(db['chickens'].rows_where('name = ?', ['BomTest2']))
        print(f'BomTest1: {bom_test1}')
        print(f'BomTest2: {bom_test2}')
        assert len(bom_test1) == 1
        assert len(bom_test2) == 1
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 8: UTF-8 BOM after empty lines (first non-empty) should be stripped')
        print('=' * 50)
        db = Database(db_path)
        count_before = db['chickens'].count
        db.close()
        
        # Empty lines before, then BOM on first non-empty line
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='\n\n\ufeff{"name": "BomTest3"}\n'
        )
        print(f'Exit code: {result.exit_code}')
        if result.exit_code != 0:
            print(f'Output: {result.output}')
        assert result.exit_code == 0
        
        db = Database(db_path)
        count_after = db['chickens'].count
        print(f'Count before: {count_before}, Count after: {count_after}')
        assert count_after == count_before + 1
        
        bom_test3 = list(db['chickens'].rows_where('name = ?', ['BomTest3']))
        print(f'BomTest3: {bom_test3}')
        assert len(bom_test3) == 1
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 9: Transaction rollback - insert error should rollback all')
        print('=' * 50)
        # Create a table with primary key constraint
        db = Database(db_path)
        db['test_pk'].create({'id': int, 'name': str}, pk='id')
        # Insert initial data
        db['test_pk'].insert({'id': 1, 'name': 'Existing'})
        initial_count = db['test_pk'].count
        db.close()
        
        # Try to insert data where the 3rd row has duplicate id
        # This should fail and rollback all inserts from this batch
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'test_pk'],
            input='{"id": 10, "name": "New1"}\n{"id": 11, "name": "New2"}\n{"id": 1, "name": "Duplicate"}\n'
        )
        print(f'Exit code: {result.exit_code}')
        print(f'Output: {result.output}')
        assert result.exit_code == 1
        # Error message should include line number
        assert 'line 3' in result.output or 'line 3' in result.output.lower()
        
        # Verify that the new rows were rolled back
        db = Database(db_path)
        final_count = db['test_pk'].count
        print(f'Initial count: {initial_count}, Final count: {final_count}')
        # Should be the same as initial count - no new rows should have been inserted
        assert final_count == initial_count
        
        # Verify only the original row exists
        rows = list(db['test_pk'].rows)
        print(f'Rows: {rows}')
        assert len(rows) == 1
        assert rows[0]['id'] == 1
        assert rows[0]['name'] == 'Existing'
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('Test 10: Successful batch insert with transaction')
        print('=' * 50)
        db = Database(db_path)
        initial_count = db['test_pk'].count
        db.close()
        
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'test_pk'],
            input='{"id": 20, "name": "Batch1"}\n{"id": 21, "name": "Batch2"}\n{"id": 22, "name": "Batch3"}\n'
        )
        print(f'Exit code: {result.exit_code}')
        if result.exit_code != 0:
            print(f'Output: {result.output}')
        assert result.exit_code == 0
        
        db = Database(db_path)
        final_count = db['test_pk'].count
        print(f'Initial count: {initial_count}, Final count: {final_count}')
        assert final_count == initial_count + 3
        
        rows = list(db['test_pk'].rows_where('id >= 20 order by id'))
        print(f'New rows: {rows}')
        assert len(rows) == 3
        db.close()
        print('PASSED\n')
        
        print('=' * 50)
        print('ALL TESTS PASSED!')
        print('=' * 50)

if __name__ == '__main__':
    main()
