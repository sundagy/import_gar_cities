import xml.etree.ElementTree as ET
from collections import deque
import mysql.connector
from tqdm import tqdm
import os
import sys

DB_CONFIG = {
    'host': 'localhost',
    'port': 9306,
    'user': 'root',
    'password': '123',
    'database': 'Manticore',
    'charset': 'utf8mb4'
}


def is_city(level: int, type: str) -> bool:
    valid_levels = {1, 2, 3, 4, 5, 6, 7}
    valid_types_level_7 = {'мкр', 'ж/р', 'тер'}
    valid_types_level_1_2 = {'г', 'г.ф.з.', 'п', 'пос'}
    invalid_types = {'АО', 'г.о.', 'м.о.', 'м.р-н', 'ф.т.', 'с.п.'}

    if level not in valid_levels:
        return False
    if level == 7 and type not in valid_types_level_7:
        return False
    if level in {1, 2} and type not in valid_types_level_1_2:
        return False
    if type in invalid_types:
        return False
    return True


def parse_and_insert(gar_folder):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Очищаем таблицу cities...")
    cursor.execute("DELETE FROM cities where country = 'RU' and id > 0")
    conn.commit()

    insert_stmt = """
        INSERT INTO cities (
            id, fias, kladr, pre, name, adm_district, mun_district, region, country, region_id, level, `long`, `lat`, postal
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    addr_objects = {}
    hierarchy_adm = {}
    hierarchy_mun = {}
    params = {}
    house_postindex = {}

    print("📥 Чтение и импорт по регионам...")
    for region_dir in sorted(os.listdir(gar_folder)):
        region_path = os.path.join(gar_folder, region_dir)
        if not os.path.isdir(region_path):
            continue

        addr_objects.clear()
        hierarchy_adm.clear()
        hierarchy_mun.clear()
        params.clear()
        house_postindex.clear()
        tree = {}

        for file in os.listdir(region_path):
            path = os.path.join(region_path, file)

            if file.startswith("AS_HOUSES_PARAMS_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'PARAM' and elem.attrib.get('TYPEID') == '5':
                        house_id = int(elem.attrib.get('OBJECTID'))
                        value = int(elem.attrib.get('VALUE'))
                        if value:
                            house_postindex[house_id] = value
                    elem.clear()

            elif file.startswith("AS_ADDR_OBJ_PARAMS_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'PARAM':
                        obj_id = int(elem.attrib.get('OBJECTID'))
                        typeid = int(elem.attrib.get('TYPEID'))
                        value = elem.attrib.get('VALUE')
                        if not typeid or not value:
                            continue
                        if obj_id not in params:
                            params[obj_id] = {'KLADR': ''}
                        if typeid == 10:
                            params[obj_id]['KLADR'] = value
                    elem.clear()

            elif file.startswith("AS_ADDR_OBJ_") and file.endswith(".XML") and "DIVISION" not in file:
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'OBJECT':
                        if elem.attrib.get('ISACTIVE') == '1' and elem.attrib.get('ISACTUAL') == '1':
                            obj_id = int(elem.attrib['OBJECTID'])
                            addr_objects[obj_id] = {
                                'ID': int(elem.attrib['ID']),
                                'OBJECTID': obj_id,
                                'OBJECTGUID': elem.attrib['OBJECTGUID'],
                                'NAME': elem.attrib.get('NAME', ''),
                                'TYPENAME': prepare_typename(elem.attrib.get('TYPENAME', '')),
                                'LEVEL': int(elem.attrib.get('LEVEL', ''))
                            }
                    elem.clear()

            elif file.startswith("AS_ADM_HIERARCHY_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'ITEM' and elem.attrib.get('ISACTIVE') == '1':
                        if 'PARENTOBJID' in elem.attrib:
                            object_id = int(elem.attrib['OBJECTID'])
                            parent_id = int(elem.attrib.get('PARENTOBJID'))
                            hierarchy_adm[object_id] = {'PARENTOBJID': parent_id}

                            if parent_id not in tree:
                                tree[parent_id] = []
                            tree[parent_id].append(object_id)
                    elem.clear()

            elif file.startswith("AS_MUN_HIERARCHY_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'ITEM' and elem.attrib.get('ISACTIVE') == '1':
                        if 'PARENTOBJID' in elem.attrib:
                            object_id = int(elem.attrib['OBJECTID'])
                            parent_id = int(elem.attrib.get('PARENTOBJID'))
                            hierarchy_mun[object_id] = {'PARENTOBJID': int(parent_id)}
                    elem.clear()

        batch = []
        count = 0
        left = 0
        for objid, obj in tqdm(addr_objects.items(), desc=f"Импорт {region_dir}"):
            if not obj['NAME'] or not obj['TYPENAME']:
                continue

            level = int(obj['LEVEL'])
            pre = obj['TYPENAME']

            if not is_city(level, pre):
                continue

            name = obj['NAME']
            fias = obj['OBJECTGUID'].replace('-', '')
            obj_id = obj['OBJECTID']
            kladr = (params[obj_id]['KLADR'] or '') if obj_id in params else ''
            region_id = int(region_dir)

            region1, adm_district, is_adm = build_hierarchy(objid, addr_objects, hierarchy_adm)
            region2, mun_district, is_mun = build_hierarchy(objid, addr_objects, hierarchy_mun)

            if not is_adm and not is_mun:
                left += 1
                continue

            post_index = find_postal(objid, addr_objects, house_postindex, tree)

            region = region1 if region1 != '' else region2

            if level == 1:
                region = ''

            batch.append((
                obj_id,
                fias,
                kladr,
                pre,
                name,
                adm_district,
                mun_district,
                region,
                'RU',
                region_id,
                level,
                0,
                0,
                post_index
            ))
            count += 1

        if batch:
            cursor.executemany(insert_stmt, batch)
            conn.commit()

        print(f"✅ Регион {region_dir}: импортировано {count} записей, left {left}")

    cursor.close()
    conn.close()
    print("🎉 Импорт завершён.")


def prepare_typename(otype: str) -> str:
    if otype.count('.') == 1:
        otype = otype.strip('.')
    return otype


def find_postal(objid, addr_objects, house_postindex, tree):
    queue = deque([objid])
    visited = set()
    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        # if addr_objects[objid]['NAME'] == 'Никель':
        #     if objid in addr_objects and current in addr_objects:
        #         print(addr_objects[objid]['NAME'], current, addr_objects[current]['NAME'])
        #     else:
        #         print(addr_objects[objid]['NAME'], current)
        #     if current in house_postindex:
        #         print(addr_objects[objid]['NAME'], current, '->', house_postindex[current])

        if current in house_postindex:
            return house_postindex[current]

        for child in tree.get(current, []):
            queue.append(child)
    return ''


def build_hierarchy(start_objid, addr_objects, hierarchy):
    visited = set()
    region = ""
    sub_region = ""
    current_id = start_objid
    is_found = False

    while current_id and current_id not in visited:
        visited.add(current_id)

        info = hierarchy.get(current_id)
        if not info:
            break

        is_found = True

        parent_id = info.get('PARENTOBJID')
        parent = addr_objects.get(parent_id)
        if parent:
            typename = parent['TYPENAME']
            level = parent['LEVEL']

            name = f"{typename} {parent['NAME']}"
            if typename in {'а.обл.', 'обл', 'край', 'округ', 'Чувашия', 'АО', 'р-н', 'Аобл', 'а.окр.', 'м.р-н'}:
                name = f"{parent['NAME']} {typename}"
            name = name.strip(' -')

            if level == 1:
                region = name
            elif level in {2, 3, 5} and not sub_region:
                sub_region = name

        current_id = parent_id

    return region, sub_region, is_found


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python import_gar.py /path/to/gar_xml")
        sys.exit(1)
    parse_and_insert(sys.argv[1])
