from setting import SDB_FILTER_DOMAIN

def get_filter_keywords(sdb):
    db_filter_domain = sdb.get_domain(SDB_FILTER_DOMAIN)
    keywords = dict()
    keywords['track'] = ''
    for filter in db_filter_domain:
        if keywords['track'] == '':
            keywords['track'] = filter['name']
        else:
            keywords['track'] += ',' + filter['name']
    return keywords
