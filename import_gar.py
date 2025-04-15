import xml.etree.ElementTree as ET
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

def parse_and_insert(gar_folder):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Очищаем таблицу cities...")
    cursor.execute("DELETE FROM cities where id > 0")
    conn.commit()

    insert_stmt = """
        INSERT INTO cities (
            id, fias, kladr, pre, name, sub_region, region, country, region_id, level, `long`, `lat`, cdek, boxberry
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    addr_objects = {}
    hierarchy = {}
    params = {}

    print("📥 Чтение и импорт по регионам...")
    for region_dir in sorted(os.listdir(gar_folder)):
        region_path = os.path.join(gar_folder, region_dir)
        if not os.path.isdir(region_path):
            continue

        addr_objects.clear()
        hierarchy.clear()
        params.clear()

        for file in os.listdir(region_path):
            path = os.path.join(region_path, file)

            if file.startswith("AS_ADDR_OBJ_PARAMS_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'PARAM':
                        obj_id = int(elem.attrib.get('OBJECTID'))
                        typeid = elem.attrib.get('TYPEID')
                        value = elem.attrib.get('VALUE')
                        if not typeid or not value:
                            continue
                        if obj_id not in params:
                            params[obj_id] = {'KLADR': None}
                        if typeid == '10':
                            params[obj_id]['KLADR'] = value
                    elem.clear()

            elif file.startswith("AS_ADDR_OBJ_") and file.endswith(".XML") and "DIVISION" not in file:
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'OBJECT':
                        if elem.attrib.get('ISACTIVE') == '1' and elem.attrib.get('ISACTUAL') == '1':
                            addr_objects[elem.attrib['OBJECTID']] = {
                                'ID': elem.attrib['ID'],
                                'OBJECTID': elem.attrib['OBJECTID'],
                                'OBJECTGUID': elem.attrib['OBJECTGUID'],
                                'NAME': elem.attrib.get('NAME', ''),
                                'TYPENAME': prepare_typename(elem.attrib.get('TYPENAME', '')),
                                'LEVEL': elem.attrib.get('LEVEL', '')
                            }
                    elem.clear()

            elif file.startswith("AS_ADM_HIERARCHY_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'ITEM' and elem.attrib.get('ISACTIVE') == '1':
                        object_id = elem.attrib['OBJECTID']
                        hierarchy[object_id] = {
                            'PARENTOBJID': elem.attrib.get('PARENTOBJID')
                        }
                    elem.clear()

        batch = []
        count = 0
        left = 0
        for objid, obj in tqdm(addr_objects.items(), desc=f"Импорт {region_dir}"):
            if not obj['NAME'] or not obj['TYPENAME']:
                continue

            level = int(obj['LEVEL'])
            pre = obj['TYPENAME']
            if level not in {1, 2, 4, 5, 6} or (level == 2 and pre != 'г') or (level == 1 and pre != 'г'):
                continue

            name = obj['NAME']
            fias = obj['OBJECTGUID'].replace('-', '')
            obj_id = int(obj['OBJECTID'])
            kladr = (params[obj_id]['KLADR'] or '') if obj_id in params else ''
            region_id = int(region_dir)

            region, sub_region, is_found = build_hierarchy(objid, addr_objects, hierarchy)

            if not is_found:
                left += 1
                continue

            batch.append((
                obj_id,
                fias,
                kladr,
                pre,
                name,
                sub_region,
                region,
                'RU',
                region_id,
                level,
                0,
                0,
                0,
                0
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
            name = f"{typename} {parent['NAME']}".strip()
            if typename != 'г':
                if level == '1':
                    region = name
                elif level in {'2', '3'} and not sub_region:
                    sub_region = name

        current_id = parent_id

    return region, sub_region, is_found


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python import_gar.py /path/to/gar_xml")
        sys.exit(1)
    parse_and_insert(sys.argv[1])
