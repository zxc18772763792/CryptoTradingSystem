from importlib.metadata import PackageNotFoundError
from scripts.check_dependency_contract import check_contract


def test_contract_detects_missing_and_wrong_versions_without_importing_packages(tmp_path):
    path=tmp_path/'requirements.txt'
    path.write_text('\ufeff# Core Dependencies\nweb3>=7.6,<8\nschedule>=1.2,<2\n# Testing\npytest>=9.0.3,<10\n',encoding='utf-8')
    versions={'web3':'8.0.0','pytest':'9.0.3'}
    def version(name):
        if name not in versions:raise PackageNotFoundError(name)
        return versions[name]
    result=check_contract(path,version=version)
    assert len(result)==2
    assert not any(row['satisfies'] for row in result)
    assert result[1]['installed'] is None
    assert check_contract(path,include_dev=True,version=version)[-1]['satisfies'] is True


def test_contract_respects_markers_and_restores_runtime_sections(tmp_path):
    path=tmp_path/'requirements.txt'
    path.write_text('# Testing\npytest>=9\n# Utilities\nrequests[socks]>=2.32; python_version >= "3"\nnever-installed; python_version < "2"\n',encoding='utf-8')
    result=check_contract(path,version=lambda name:'2.34.2')
    assert [row['name'] for row in result]==['requests']
    assert result[0]['satisfies']
