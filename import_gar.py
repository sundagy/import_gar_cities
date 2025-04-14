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

TARGET_LEVELS = {'4', '5', '6'}

def parse_and_insert(gar_folder):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Очищаем таблицу cities...")
    cursor.execute("DELETE FROM cities where id > 0")
    cursor.close()
    conn.commit()

    insert_stmt = """
        INSERT INTO cities (
            id, fias, kladr, pre, name, sub_region, region, country, region_id, `long`, `lat`, cdek, boxberry
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    addr_objects = {}
    hierarchy = {}
    params = {}

    cursor = conn.cursor()
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
                                'TYPENAME': elem.attrib.get('TYPENAME', ''),
                                'LEVEL': elem.attrib.get('LEVEL', '')
                            }
                    elem.clear()

            elif file.startswith("AS_ADM_HIERARCHY_") and file.endswith(".XML"):
                for event, elem in ET.iterparse(path, events=("end",)):
                    if elem.tag == 'ITEM' and elem.attrib.get('ISACTIVE') == '1':
                        object_id = elem.attrib['OBJECTID']
                        hierarchy[object_id] = {
                            'PARENTOBJID': elem.attrib.get('PARENTOBJID'),
                            'REGIONCODE': elem.attrib.get('REGIONCODE')
                        }
                    elem.clear()

        count = 0
        for objid, obj in tqdm(addr_objects.items(), desc=f"Импорт {region_dir}"):
            level = obj['LEVEL']
            if level not in TARGET_LEVELS:
                continue
            if not obj['NAME'] or not obj['TYPENAME']:
                continue

            kladr = ''
            pre = obj['TYPENAME']
            name = obj['NAME']
            region, sub_region, region_id = build_hierarchy(objid, addr_objects, hierarchy)
            fias = obj['OBJECTGUID'].replace('-', '')
            obj_id = int(obj['OBJECTID'])

            if obj_id in params:
                kladr = params[obj_id]['KLADR'] or ''

            if region_id is None:
                continue

            cursor.execute(insert_stmt, (
                obj_id,
                fias,
                kladr,
                pre,
                name,
                sub_region,
                region,
                'RU',
                region_id,
                0,
                0,
                0,
                0
            ))
            count += 1

        conn.commit()
        print(f"✅ Регион {region_dir}: импортировано {count} записей")

    cursor.close()
    conn.close()
    print("🎉 Импорт завершён.")

def build_hierarchy(start_objid, addr_objects, hierarchy):
    visited = set()
    region = ""
    sub_region = ""
    region_id = None
    current_id = start_objid

    while current_id and current_id not in visited:
        visited.add(current_id)
        info = hierarchy.get(current_id)
        if not info:
            break
        parent_id = info.get('PARENTOBJID')
        parent = addr_objects.get(parent_id)
        if parent:
            level = parent['LEVEL']
            name = f"{parent['TYPENAME']} {parent['NAME']}".strip()
            if level == '1':
                region = name
                region_id = int(info['REGIONCODE']) if info.get('REGIONCODE', '').isdigit() else None
            elif level == '2' and not sub_region:
                sub_region = name
        current_id = parent_id

    return region, sub_region, region_id

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python import_gar.py /path/to/gar_xml")
        sys.exit(1)
    parse_and_insert(sys.argv[1])
