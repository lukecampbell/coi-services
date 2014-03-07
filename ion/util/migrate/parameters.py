#!/usr/bin/env python

__author__ = 'Luke'

example_pfunc = {'_id': '22df655600214ac3a3287c11141c18e8',
 '_rev': '2',
 'addl': {},
 'alt_ids': ['PRE:PFID20'],
 'availability': 'AVAILABLE',
 'description': '',
 'lcstate': 'DEPLOYED',
 'name': 'dataqc_polytrendtest',
 'parameter_function': {'_id': None,
  'arg_list': ['dat', 't', 'ord_n', 'ntsd'],
  'cm_type': ['coverage_model.parameter_functions', 'PythonFunction'],
  'extension': {},
  'func_name': 'dataqc_polytrendtest_wrapper',
  'kwarg_map': None,
  'mutable': False,
  'name': 'dataqc_polytrendtest',
  'owner': 'ion_functions.qc.qc_functions',
  'param_map': None},
 'ts_created': '1391556908236',
 'ts_updated': '1391556908282',
 'type_': 'ParameterFunction'}

example_numexpr = {u'_id': u'b8a90556e8e14971abdb9d06e7f59255',
 u'_rev': u'2',
 u'addl': {},
 u'alt_ids': [u'PRE:PFID2'],
 u'availability': u'AVAILABLE',
 u'description': u'',
 u'lcstate': u'DEPLOYED',
 u'name': u'CONDWAT_L1',
 u'parameter_function': {u'_id': None,
  u'arg_list': [u'C'],
  u'cm_type': [u'coverage_model.parameter_functions', u'NumexprFunction'],
  u'expression': u'(C / 100000) - 0.5',
  u'extension': {},
  u'mutable': False,
  u'name': u'CONDWAT_L1',
  u'param_map': None},
 u'ts_created': u'1391557033976',
 u'ts_updated': u'1391557034019',
 u'type_': u'ParameterFunction'}


standard_keys = ['_id', '_rev', 'addl', 'alt_ids', 'availability', 'description', 'lcstate', 'name', 'ts_created', 'ts_updated', 'type_']

def migrate_parameter_function(doc):
    '''
    Returns a corrected schema doc based on the schema in
    https://github.com/ooici/ion-definitions/blob/5468811/objects/data/dm/parameter.yml 
    '''
    retval = {}
    for key in standard_keys:
        retval[key] = doc[key]

    try:
        if doc['parameter_function']['cm_type'][1] == 'PythonFunction':
            retval['function_type'] = 0
            retval['function'] = doc['parameter_function']['func_name']
            retval['owner'] = doc['parameter_function']['owner']
            retval['args'] = doc['parameter_function']['arg_list']
            retval['egg_uri'] = '' 
        elif doc['parameter_function']['cm_type'][1] == 'NumexprFunction':
            retval['function_type'] = 1
            retval['function'] = doc['parameter_function']['expression']
            retval['args'] = doc['parameter_function']['arg_list']
            retval['egg_uri'] = ''
    except:
        print doc
        from traceback import print_exc
        print_exc()


    return retval


if __name__ == '__main__':
    import psycopg2 as pg 
    import simplejson as json
    def main():
        conn = pg.connect("dbname=ion_migrate user=luke port=5433")
        cursor = conn.cursor()

        cursor.execute("SELECT doc FROM ion_resources WHERE type_='ParameterFunction'")
        for row in cursor.fetchall():
            doc, = row
            corrected = migrate_parameter_function(doc)
            cursor.execute("INSERT INTO ion_resources_copy(id,rev,doc,type_,lcstate,availability,name,ts_created,ts_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (corrected['_id'],
                     corrected['_rev'],
                     json.dumps(corrected),
                     corrected['type_'],
                     corrected['lcstate'],
                     corrected['availability'],
                     corrected['name'],
                     corrected['ts_created'],
                     corrected['ts_updated']))
        conn.commit()
        conn.close()
    main()

