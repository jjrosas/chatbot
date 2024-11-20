from datetime import datetime, timedelta


start_timestamp = datetime.now()
days_before = 30
schema = 'predize_info'
table_name_tickets ='tickets'
table_name_messages = 'messages'

updated_from_date = (datetime.today() - timedelta(days = days_before)).strftime('%Y-%m-%d')

end_date = (datetime.today() +timedelta(days=1)).replace(hour=0,microsecond=0,minute=0,second=0)

range_dates = sorted([(end_date- timedelta(days=x)).strftime('%Y-%m-%d')  for x in range(0,10) ])
