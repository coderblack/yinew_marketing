<#list events as eparam >
select
    deviceId,
    count(1) as cnt
from yinew_detail
where
      deviceId = ?
  and eventId='${eparam.eventId}'
    <#list eparam.properties?keys as key>
        and  properties['${key}']='${eparam.properties[key]}'
    </#list>
  and timeStamp>= ?
  and timeStamp <= ?
group by deviceId;
</#list>