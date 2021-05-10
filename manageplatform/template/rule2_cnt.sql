select
    deviceId,
    count(1) as cnt
from yinew_detail
where deviceId = ? and eventId = 'H' and properties['p1']='v1'
  and timeStamp between ? and ?
group by deviceId