from typing import List, Union, Any, Optional
import re

from lib.nsw_vg.property_description import types as t
from lib.nsw_vg.property_description import grammar as g

def parse_land_parcel_ids(desc: str):
    def read_chunk(read_from, skip = 0):
        copy = desc[read_from:]
        while skip > 0:
            copy = copy[copy.find(' ') + 1:]
            skip -= 1
        if copy.find(' ') == -1:
            return copy
        else:
            return copy[:copy.find(' ')]
        
    def move_cursor(read_from, skip = 0):
        while skip > 0:
            if desc[read_from:].find(' ') == -1:
                return len(desc)
                
            read_from = read_from + desc[read_from:].find(' ') + 1
            skip -= 1
        return read_from
        
    def impl():
        read_from = 0
        chunk = None
        
        while read_from < len(desc):
            chunk = read_chunk(read_from, skip=0)
            # print(read_from, desc[read_from:], chunk, f"'{read_chunk(read_from, skip=1)}'")
            
            if '/' in chunk:
                yield t.LandParcel(id=chunk, part=False)
                read_from = move_cursor(read_from, 1)
                continue
                
            if 'PT' == chunk and '/' in read_chunk(read_from, skip=1):
                yield t.LandParcel(id=read_chunk(read_from, skip=1), part=True)
                read_from = move_cursor(read_from, 2)
                continue

            if 'PT' != chunk and not chunk.endswith(','):
                return desc[read_from:] 

            lots = []
            plan = ''
            while True:                    
                chunk = read_chunk(read_from)
                part = False
                
                if chunk == 'PT':
                    part = True
                    read_from = move_cursor(read_from, 1)
                    chunk = read_chunk(read_from)
                    
                if chunk.endswith(','):
                    lots.append((part, chunk[:-1]))
                    read_from = move_cursor(read_from, 1)
                    continue  
                elif '/' in chunk:
                    lots.append((part, chunk[:chunk.find('/')]))
                    plan = chunk[chunk.find('/'):]
                    read_from = move_cursor(read_from, 1)
                    break
                else:
                    return desc[read_from:]
                    
            for part, lot in lots:
                yield t.LandParcel(id=f'{lot}{plan}', part=part)
        return desc[read_from:]
    
    land_parcels: List[t.LandParcel] = []
    gen = impl()
    
    try:
        while True:
            land_parcels.append(next(gen))
    except StopIteration as e:
        return e.value, land_parcels

def parse_property_description(description: str) -> List[t.ParsedItem]:
    description = re.sub(r'\s+', ' ', description)
    parsed_items: List[t.ParsedItem] = []

    for pattern in g.ignore_pre_patterns:
        description = pattern.sub('', description)

    for pattern in g.id_patterns:
        for match in pattern.re.finditer(description):
            parsed_items.append(pattern.Const(id=match.group(1)))
        description = pattern.re.sub('', description)

    for pattern in g.named_group_patterns:
        for match in pattern.re.finditer(description):
            parsed_item = pattern.Const(
                **{ k: match.group(k) for k in pattern.id_names },
                **{ 
                    k: match.group(k) is not None
                    for k in pattern.bool_names
                },
            )
            parsed_items.append(parsed_item)
        description = pattern.re.sub('', description)

    for pattern in g.flag_patterns:
        for match in pattern.re.finditer(description):
            parsed_items.append(pattern.Const())
        description = pattern.re.sub('', description)

    for pattern in g.ignore_post_patterns:
        description = pattern.sub('', description)
        
    description = re.sub(r'\s+', ' ', description)
    description, land_parcels = parse_land_parcel_ids(description)
    description = re.sub(r'(\s+|\.)+', '', description)
    parsed_items.extend(land_parcels)
    return description, parsed_items
