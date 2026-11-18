"""Kiểm tra cửa sổ live trong MongoDB: số zone, số đoạn, số zone chậm bất thường (< 0,7) ở vài cửa sổ."""
import sys, json
from core.common import load_config
from pymongo import MongoClient
import pandas as pd
cfg=load_config(); db=MongoClient(cfg['mongo']['uri'])[cfg['mongo']['database']]
live=pd.DataFrame(list(db.zone_speed_live.find({},{'_id':0})))
zhs=pd.DataFrame(list(db.zone_hour_speed.find({},{'_id':0})))
by=live.groupby('window_end').agg(zones=('zone_id','count'),segs=('n_segments','sum')).tail(5)
print(by.to_string())
def slow_share(we):
    w=live[live.window_end==we].copy(); h=w.window_start.iloc[0].hour
    m=w.merge(zhs[(zhs.hour==h)&(~zhs.is_weekend)],on='zone_id',suffixes=('','_h'))
    m['ratio']=(m.total_km/(m.total_s/3600))/(m.total_km_h/(m.total_s_h/3600))
    return {'window_end':str(we),'zones':len(w),'compared':len(m),'slow_lt_0_7':int((m.ratio<0.7).sum()),'median_ratio':round(float(m.ratio.median()),3)}
ws=sorted(live.window_end.unique())
for we in [ws[-1],ws[-2],ws[len(ws)//2]]: print(slow_share(we))
