from datetime import datetime
from pathlib import Path
import os
import re

from lib.nsw_vg.property_sales import types as ps

SYNTAX_1990 = { 'A': 5, 'B': 21, 'Z': 3 }
SYNTAX_2002 = { 'A': 4, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }
SYNTAX_2012 = { 'A': 5, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }
SYNTAX_2021 = { 'A': 5, 'B': 24, 'C': 6, 'D': 12, 'Z': 5 }

def get_data_from_targets(parent_dir, targets):
    for target in targets:
        directory = Path(f'{parent_dir}/{target.zip_dst}')
        item = None
        file = None

        try:
            for file in directory.rglob('*.DAT'):
                file = str(file)
                
                if file.endswith('-checkpoint.DAT'):
                    continue
                if not os.path.isfile(file):
                    continue

                year = target.datetime.year
                download_date = _get_download_date(file)
                columns, syntax = get_columns_and_syntax(download_date, year)
                if syntax == None:
                    continue
            
                yield from get_data_from_file(columns, syntax, file, year)
        except Exception as e:
            print(file, item)
            raise e
    

def get_data_from_file(columns, syntax, file_path, year):
    reader = PropertySalesRowReader(syntax)
    kind, row = None, None
    
    try:
        a = None
        b = None
        c = None
        
        for kind, row in reader.get_rows(file_path):
            if kind == 'A':
                a = columns['A'].from_row(row, file_path=file_path, year=year)
                yield a
            elif kind == 'B':
                b = columns['B'].from_row(row, parent=a)
                yield b
            elif kind == 'C':
                c = columns['C'].from_row(row, parent=b)
                yield c
            elif kind == 'D':
                yield columns['D'].from_row(row, parent=c)
            elif kind == 'Z':
                yield columns['Z'].from_row(row, parent=a)
            else:
                raise ValueError(f"Unexpected record type: {kind}")
    except Exception as e:
        print(kind, row, file_path, reader.debug_info())
        raise e

def _get_download_date(file_path):
    if re.search(r"_\d{8}\.DAT$", file_path):
        file_date = file_path[file_path.rfind('_')+1:file_path.rfind('.')]
        return datetime.strptime(file_date, "%d%m%Y")
    else:
        return None

def get_columns_and_syntax(download_date, year):
    if year <= 2001 and not download_date:
        return ps.t_1990, SYNTAX_1990 
    elif year == 2001:
        if download_date.year > 2001:
            return ps.t_2002, SYNTAX_2002 
        elif download_date.month < 6:
            return ps.t_1990, SYNTAX_1990 
        elif download_date.month < 9:
            return None, None
        else:
            return ps.t_2002, SYNTAX_2002 
    elif year < 2012:
        return ps.t_2002, SYNTAX_2002
    elif year == 2012:
        if download_date.month < 3:
            return ps.t_2002, SYNTAX_2002
        elif download_date.month == 3 and download_date.day < 13:
            return ps.t_2002, SYNTAX_2002
        else:
            return ps.t_current, SYNTAX_2012  
    elif year == 2021 and download_date.month == 8 and download_date.day == 23:
        return ps.t_current, SYNTAX_2021
    else:
        return ps.t_current, SYNTAX_2012

class PropertySalesRowReader:
    def __init__(self, semicolons):
        self._semicolons = semicolons
        self._buffer = ""
        self._index = 0

    def get_rows(self, file_path):
        with open(file_path, 'r') as file:
            self._buffer = file.read()
            
        while self._index < len(self._buffer):
            mode = self._buffer[self._index:self._index + 1].strip()
            if mode not in self._semicolons:
                raise ValueError(f"Unexpected mode: {mode}@{self._index}")

            row_end = self._find_nth_semicolon_2(self._semicolons[mode])
            if row_end == -1:
                raise ValueError('Unexpected end of data')

            yield mode, self._buffer[self._index + 2: row_end].split(';')
            if mode == 'Z':
                break
                
            self._index = row_end + 2

    
    def _find_nth_semicolon_2(self, n):
        position = self._index
        semicolon_count = 0
        while semicolon_count < n:
            found_index = self._buffer.find(';', position)
            if found_index == -1:
                return -1
            position = found_index + 1
            semicolon_count += 1
        return found_index

    def debug_info(self):
        return f"""
          index @ {self._index}
          semicolumns @ {self._semicolons}
          REMAINING: {self._buffer[self._index:self._index + 400]}
        """

        