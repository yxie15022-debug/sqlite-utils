from sqlite_utils import cli, Database
from click.testing import CliRunner
import json
import tempfile
import os

def test_ingest():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, 'test.db')
        
        print('测试1: 表不存在时报错')
        result = CliRunner().invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='{"name": "Lila"}\n'
        )
        print(f'  退出码: {result.exit_code}')
        print(f'  输出: {result.output.strip()}')
        assert result.exit_code == 1
        assert "does not exist" in result.output
        print('  ✓ 通过')
        
        db = Database(db_path)
        db['chickens'].create({'name': str})
        
        print('\n测试2: 成功插入数据到已存在的表')
        result = CliRunner().invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='{"name": "Lila"}\n{"name": "Cleo"}\n'
        )
        print(f'  退出码: {result.exit_code}')
        if result.exit_code != 0:
            print(f'  错误: {result.output}')
        assert result.exit_code == 0
        rows = list(db['chickens'].rows)
        print(f'  插入的行: {rows}')
        assert len(rows) == 2
        assert rows[0]['name'] == 'Lila'
        assert rows[1]['name'] == 'Cleo'
        print('  ✓ 通过')
        
        print('\n测试3: 自动添加新列（TEXT类型）')
        result = CliRunner().invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='{"name": "Suna", "age": 3, "weight": 4.5}\n'
        )
        print(f'  退出码: {result.exit_code}')
        if result.exit_code != 0:
            print(f'  错误: {result.output}')
        assert result.exit_code == 0
        
        columns = [c.name for c in db['chickens'].columns]
        print(f'  列: {columns}')
        assert 'name' in columns
        assert 'age' in columns
        assert 'weight' in columns
        
        for col in db['chickens'].columns:
            print(f'  列 {col.name} 类型: {col.type}')
        
        rows = list(db['chickens'].rows_where('name = ?', ['Suna']))
        print(f'  新插入的行: {rows}')
        assert len(rows) == 1
        assert rows[0]['age'] == 3
        assert rows[0]['weight'] == 4.5
        print('  ✓ 通过')
        
        print('\n测试4: 无效JSON报错')
        result = CliRunner().invoke(
            cli.cli,
            ['ingest', db_path, 'chickens'],
            input='not valid json\n'
        )
        print(f'  退出码: {result.exit_code}')
        print(f'  输出: {result.output.strip()}')
        assert result.exit_code == 1
        assert 'Invalid JSON' in result.output
        print('  ✓ 通过')
        
        print('\n✓ 所有测试通过！')

if __name__ == '__main__':
    test_ingest()
