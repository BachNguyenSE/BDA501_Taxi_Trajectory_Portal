"""Giải thích bản trễ không bị bỏ: tái tạo plan của producer (cùng seed) và so với watermark cao nhất có thể đạt.
Chạy: python src/checks/explain_late.py"""
import sys, json
from core.common import ROOT, load_config
from streaming.producer import load_segments, build_plan, window_bounds, passes_filter
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
cfg=load_config(); tz=ZoneInfo('Europe/Lisbon')
segs,_=load_segments(cfg, ROOT/cfg['paths']['raw_csv']); plan,man=build_plan(segs,cfg,cfg['project']['seed'])
late=[json.loads(v) for c,k,v,kind in plan if kind=='late']
ontime_max=max(json.loads(v)['event_time'] for c,k,v,kind in plan if kind in('normal','duplicate_original'))
wm=ontime_max-600   # watermark cao nhất có thể đạt được trước message kết thúc
tail=[m for m in late if m['event_time']>=wm]
f=lambda t: datetime.fromtimestamp(t,tz).strftime('%H:%M:%S')
print(json.dumps({'late_total':len(late),'max_on_time_event_local':f(ontime_max),'max_watermark_before_sentinel_local':f(wm),
 'late_with_event_after_that_watermark':len(tail),'of_which_pass_filter':sum(passes_filter(m,cfg) for m in tail),
 'their_event_times':sorted(f(m['event_time']) for m in tail)},indent=1,ensure_ascii=False))
