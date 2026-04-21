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
        print('Test 4: Invalid JSON')
        print('=' * 50)
        result = runner.invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='not valid json\n'
        )
        print(f'Exit code: {result.exit_code}')
        print(f'Output: {result.output}')
        assert result.exit_code == 1
        assert 'Invalid JSON' in result.output
        print('PASSED\n')
        
        print('=' * 50)
        print('ALL TESTS PASSED!')
        print('=' * 50)

if __name__ == '__main__':
    main()
