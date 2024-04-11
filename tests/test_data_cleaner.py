import pytest

from service.data_cleaner import MetaDataFromArchiveCleaner

def test_run_cleaning_file_not_found(archive_cleaner):
    path_to_archive = 'NOT_SET'
    mdfa_cleaner = MetaDataFromArchiveCleaner(path_to_archive)

    with pytest.raises(FileNotFoundError) as excinfo:
        mdfa_cleaner.run_cleaning()   
    
    assert f"Meta data archive '{path_to_archive}' not found." in str(excinfo.value)
