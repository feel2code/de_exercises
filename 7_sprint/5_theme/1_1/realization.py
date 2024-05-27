from datetime import datetime, timedelta

def input_paths(date, depth):
    list_path = []
    for i in range(depth):
        dtr = str((datetime.strptime(date, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d"))
        list_path.append(f'/user/USER/data/events/date={dtr}/event_type=message')
    return list_path

