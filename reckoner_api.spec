# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = []
binaries = []
hiddenimports = ['adapter', 'duckdb_adapter', 'postgres_adapter', 'osi_parser', 'dbt_parser', 'mb8_data_connect', 'psycopg2', 'sqlalchemy']
tmp_ret = collect_all('snf_peirce')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['reckoner_api.py'],
    pathex=['C:\\Users\\Alexander\\Desktop\\Data Project - SNF\\snf-toolkit\\reckoner', 'C:\\Users\\Alexander\\Desktop\\Data Project - SNF\\snf-py'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='reckoner_api',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='reckoner_api',
)
