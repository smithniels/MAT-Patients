----------------------------------------------------------------------------------
------ Niels' Slightly Longer Method for Pulling Data on MAT patients ------------
----------------------------------------------------------------------------------

--Temporary Tables! So momentary! Wow! 
if object_id('tempdb..#temptable1') is not null 
	drop table #temptable1
if object_id('tempdb..#temptable2') is not null 
	drop table #temptable2
if object_id('tempdb..#temptable3') is not null 
	drop table #temptable3
if object_id('tempdb..#temptable4') is not null
	drop table #temptable4
if object_id('tempdb..#temptable50') is not null
	drop table #temptable50 

-- Set Date Range Here!
DECLARE @STARTDATE AS DATE
DECLARE @ENDDATE AS DATE
SET @STARTDATE = '01/01/2020'
SET @ENDDATE   = '01/31/2020'



-- Part 1 / One / Uno /  いち (Ichi)
-- (1) pull ALL MAT visits & put into #temptable1
select p.controlno, u.ufname, u.ulname, convert(date,e.date) date, e.visittype, e.encounterID,

-- (1.1) assign a count for each individual pt's visit (creating a 1,2,3...x ranking) 
--		example: A patient's third ever visit gets a 3 in the ROW_NUM column, then their fourth visit gets a 4, fifth 5 ... etc etc!
ROW_NUMBER() OVER 
				  (PARTITION BY p.pid  -- SPLIT INTO GROUPS BASED ON P.PID
				  ORDER BY e.date      -- ORDER RESULTS BY THE VISIT DATE(E.DATE)
				  ) AS ROW_NUM		   -- ASSIGN COUNT NUMBERS TO NEW COLUMN NAMED "ROW_NUM"
into #temptable1
from patients p, users u, enc e
where p.pid = u.uid
and e.patientid = u.uid
AND E.status = 'CHK'
AND u.ulname <> 'test'
AND E.deleteFlag <> 1
and e.visittype like 'MAT'
group by p.controlno, p.pid, u.ufname, u.ulname, e.date, e.visittype, e.encounterID
select * from #temptable1

select  controlno, ufname, ulname, visittype, max(row_num) as num_vis
from #temptable1
group by controlno, ufname, ulname, visittype




-- Part 2 / Two / Dos / に (Ni)
-- (1) take everything in #temptable1 and put it into #temptable2
Select * 
	into #temptable2 
	from #temptable1

-- (2) from #temptable2, delete all NON-first-ever visits keeping only each MAT pts's first ever MAT visit
Delete 
	from #temptable2 
	where #temptable2.row_num > 1




-- Part 3 / Three / Tres / さん (San)
-- (1) count number of MAT patients & count number of MAT visits
select count(distinct t1.controlNo) 'Total_MAT_Patient_Count', count(t1.encounterid) 'Total_MAT_Visit_Count'
from #temptable1 t1

-- (2) count number of MAT patients & count number of MAT visits IN RANGE!
select count(distinct t1.controlno) 'Total_MAT_Patient_Count_in_Range',count(t1.controlno) 'Total_MAT_Visit_Count_in_Range'
from #temptable1 t1
where t1.date between @STARTDATE AND @ENDDATE




-- Part 4 / Four / Quatro / よん (Yon)
-- (1) pull only MAT client's whose first MAT vist is in the period
-- This will limit the result count to only client's whose FIRST visit falls within the range
select distinct t2.controlNo, t2.ufname, t2.ulname, t2.date, t2.encounterID, t2.VisitType, t2.Row_num

-- (2) Put that into #temptable3
into #temptable3
from #temptable2 t2
where t2.date between @STARTDATE AND @ENDDATE

-- (3) Pull count of first time MAT pts
select count(distinct t2.controlno) 'Firsttime_MAT_Patient_MAT_Visit_in_Range' --too many words here ._.
from #temptable2 t2
where t2.date between @STARTDATE AND @ENDDATE




-- Part 5 / Five / Cinco / ご (Go)
-- (1) Pull a list of all medical patient / visit info
select  p.controlno, u.ufname, u.ulname, u.dob , Min(e.dATE) as first_med_vis

-- (2) Put that into #temptable4
into #temptable4
from patients p, users u, enc e
where p.pid=u.uid
and p.pid=e.patientid
and (
	e.visittype in ('ADULT-FU','ADULT-NEW','ADULT-PE','ADULT-URG','Asylum Ex','CNSL-INd','Deaf-FU',
					'Deaf-New', 'DEN-FU', 'DEN-NEW','DEN-PO', 'DEN-RCT', 'DEN-Rec',
					'email-su', 'hlthed-ind', 'hrr-piq', 'GYN-FU','GYN-NEW', 'LAB ONLY','NURSE',
					'PED-PRENAT','PEDS-FU','PEDS-PE','PEDS-URG','RCM-OFF')
	or e.visittype like 'BH%'
	or e.visittype like 'DEN%'	
	or e.visittype like 'EYE%'
	)
and e.deleteflag = '0'
and e.status ='CHK'
and u.ulname <>'Test'
and e.date < @ENDDATE -- This quick-fix could be improved -- jk
group by p.controlno, u.ufname, u.ulname, u.dob 




-- Part 6 / six / seis / ろく (Roku)
-- (1) 
select t2.controlno, t2.ufname, t2.ulname, convert(date,t4.first_med_vis) as date_first_MED_vis, t2.date as date_first_mat,
		case when t4.first_med_vis < t2.date then 0
			 else 1
			 end as med_pat_status,
		case when t2.date between @STARTDATE AND @ENDDATE THEN 1 else 0 end as 'first-ever_Mat_in_range'
into #temptable50
from #temptable2 t2
left join #temptable4 t4 on t2.controlno = t4.controlno
order by t4.first_med_vis desc, t2.controlno, t2.ufname, t2.ulname, t2.date
-- 1400044483

--(2) pull count of patients who came for mat and have never been to BFTC)
select sum(t50.med_pat_status) as num_new_mat_bftc_pat_status
from #temptable50 t50

--(3) pull count of patients who came for mat and have never been to BFTC IN RANGE) 
select sum(t50.[first-ever_Mat_in_range]) as num_first_ever_mat_in_range
from #temptable50 t50
--where 



-- Part 7 / seven / sietea / なな (Nana)
-- CODE FOR PTS WITH SUBSTANCE USE DISORDER DX (Code pulled from "2018 pts with substance use disorder.sql"
-- This also includes non-opioids. Should it only include diagnosis like '%Opioid%'?
select distinct /*id. value,*/ p.controlno, u.ufname, u.ulname, u.dob--, i.itemname as diagnosis, id.value as ICD
from patients p, users u, doctors d, enc e, diagnosis dx, items i , itemdetail id 
where p.pid=e.patientid
and e.encounterid=dx.encounterid
and e.doctorid=d.doctorid
and dx.itemid=i.itemid 
and i.itemid=id.itemid 
and i.itemname<>''
and u.uid=p.pid
and e.date between @STARTDATE and @ENDDATE
and e.deleteflag='0'
and e.status='CHK'
and (  id.value like 'F11%' 
	or id.value like 'F12%' 
	or id.value like 'F13%' 
	or id.value like 'F14%' 
	or id.value like 'F15%' 
	or id.value like 'F16%' 
	or id.value like 'F18%' 
	or id.value like 'F19%' 
	or id.value like 'G62.0'  
	or id.value like 'O99.32%'
	or id.value like 'Z51.81' --this is the ICD code for giving soboxone (sp?)
	)
and (e.visittype in (
					'ADULT-FU','ADULT-NEW','ADULT-PE','ADULT-URG','Asylum Ex','CNSL-INd','Deaf-FU',
					'Deaf-New', 'DEN-FU', 'DEN-NEW','DEN-PO', 'DEN-RCT', 'DEN-Rec',
					'email-su', 'hlthed-ind', 'hrr-piq', 'GYN-FU','GYN-NEW', 'LAB ONLY','NURSE',
					'PED-PRENAT','PEDS-FU','PEDS-PE','PEDS-URG','RCM-OFF', 'MAT') /* Same as above, but with MAT*/
	or e.visittype like 'BH%'
	or e.visittype like 'DEN%'	
	or e.visittype like 'EYE%'
	)
group by 
p.controlno, u.ufname, u.ulname, u.dob
