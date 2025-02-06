

--select  * from DBOps.dbo.DRC_MD1;
--select  * from DBOps.dbo.DR_Outcome_2;
--select  * from DBOps.dbo.DRC_MD1;

drop table   DBOps.dbo.DRC_MD1;
--select * from DetailedRequest where clientid = 545


--select * from ReviewStat..DetailedRequest where ExternalDetailedRequestId like '%545MDR-'



---------------------------------------------------Step1--------------------------------------------------


declare @fileName varchar (100)

set @fileName= 'DRC_MD1' 


--import from temp table ##MitchellUrdSampleData (Ahshay EDI Sample file)

insert into  FileBatch (InputId, FileName, InProcess, ReceivedDate, CompleteDate, ClientId, BatchAckDirectoryFolder, IsBulkLoad)

			--values 	('2029', 'DRC_MD1', 0 ,	getdate(),	NULL	,'545',	NULL, 1 ) --FOR UAT
			values 	('21', 'DRC_MD1'	,  0 ,	getdate(),	NULL	,'545',	NULL, 1 ) --FOR PROD

-- select auth_start_date , case when auth_end_date like '%00000%' then null else auth_end_date end as auth_end_date from ##MitchellUrdSampleData

declare @filebatchid int 
select @filebatchid= max(filebatchid) from FileBatch where ClientId = '545' AND filename like '%DRC_MD1%'
--select @filebatchid

select distinct
	'' as ReviewId,
	MDRID as ExternalReviewId, ---- Data from Ahshay
	null as Id,
	ExternalDetailedRequestID
 as ExternalDetailedRequestId,
 null as DosStart,
 null as DosEnd,
	--convert(datetime, ServiceFrom ) as DosStart,-- Data from Ahshay
	--convert(datetime, ServiceThru ) as DosEnd,-- Data from Ahshay
	'' as   TreatmentId ,----treatment_id as TreatmentId, -- Data from Ahshay --update by dennis on 14 june 18
	null as SubTreatmentId,
	null as CPTId,
	--RequestedServiceCodeFrom as CPTId,-- Data from Ahshay
	null as Units,
	null as AuthUnits,
	--cast (cast( RequestedTotalUnits  as float )as decimal(10,2) )as Units,-- Data from Ahshay
	--cast (cast(AuthorizedTotalUnits as float )as decimal(10,2))as AuthUnits,-- Data from Ahshay
	null as ICD9Id,
	 --When LTRIM(RTRIM(tr_decision)) = 'Administrative Denial' then '1000000044'
	     --When LTRIM(RTRIM(tr_decision)) = 'MMI reached' then '1000000046'
	case    when  LTRIM(RTRIM(outcome1)) = 'Denied' then    '1000000046' 
			WHEN  LTRIM(RTRIM(outcome1)) = 'Approved' then '18' 
		--When  LTRIM(RTRIM(tr_decision)) = 'Duplicate precert' then '1000000042'
		WHEN  LTRIM(RTRIM(outcome1)) = 'Modified' THEN  '117' end as DeterminationId,
        --WHEN  LTRIM(RTRIM(tr_decision)) =  'Claims Decision' then '1000000047' end as DeterminationId,
			--WHEN  LTRIM(RTRIM(outcome1)) = 'Service Cancelled' then '9' end as DeterminationId,

  --null as DeterminationId,
		--WHEN  LTRIM(RTRIM(Det_ID)) = 'ADDENDUM' THEN  '120'
		--WHEN  LTRIM(RTRIM(Det_ID)) = 'DONE' THEN  '119'
		--ELSE  '' end as DeterminationId, -- hardcoded for a time being
		--WHEN  LTRIM(RTRIM(OUTCOME)) = ''  OR  OUTCOME IS NULL OR  OUTCOME = 'INSUFFICIENT INFORMATION' THEN '-999'                                                                                	
		--ELSE  LTRIM(RTRIM(OUTCOME))	end as DeterminationId, -- hardcoded for a time being
	null as GuidelineId,
	null as Provider,
	null as Cost,
	null as CostSavings,
	'0' as ProcessStatus,
	--ExternalDetailedRequestID as DetailedRequestId,
	null as Seq,
	getdate () as CreateDateCst,
	'545' as ClientId,
	@filebatchid as FileBatchId,
	--1407 as FileBatchId, 
	MDRReason1 as DescriptionText,-- Data from Ahshay
	null as BodyPartId,
	null as CptNdcModifier,
	null as AuthorizedCpt,
	null as AuthorizedCptNdcModifier,
	null as ErrorMessage,
	null as Color,
	null as SideId
	----row_number () over (order by record_type) as rownum 
into   ##MitchellUrdSampleDataDR
from  DBOps.dbo.DRC_MD1;


--drop table DBOps.dbo.U_DR_GB;
 -- drop table ##MitchellUrdSampleDataDR
--  select * from ##MitchellUrdSampleDataDR --215
--  select * from ##MitchellUrdSampleDataDR where DeterminationId='INSUFFICIENT INFORMATION'

----====================================================================================================================
---- select convert(datetime, auth_start_date )   from ##MitchellUrdSampleData
---- select convert(datetime, auth_end_date )   from ##MitchellUrdSampleData
---- select top 131244 auth_start_date from ##MitchellUrdSampleData
----  select   auth_start_date from ##MitchellUrdSampleData where  auth_start_date like '0%'
--  select   auth_end_date from ##MitchellUrdSampleData where  auth_end_date like '0%'
----select auth_start_date from ##MitchellUrdSampleData where  auth_start_date  = '02010816'

--select * from ##MitchellUrdSampleDataReplica
  --update ##MitchellUrdSampleData set auth_start_date = null where auth_start_date = '00000000'
  --update ##MitchellUrdSampleData set auth_start_date = null where auth_start_date = '02010816'
  --update ##MitchellUrdSampleData set auth_start_date = null where auth_start_date like '0%'
  --update ##MitchellUrdSampleData set auth_end_date = null where auth_end_date = '00000000' 
  --update ##MitchellUrdSampleData set auth_end_date = null where auth_end_date like '0%' 
 ---- -- update  ##MitchellUrdSampleDataDR  set DeterminationId   = '1' where  rownum%2 =0
--====================================================================================================================
--cptid update
--select CPTID from ##MitchellUrdSampleDataDR where len(cptid)=2  
----1.--update ##MitchellUrdSampleDataDR set CPTID='' where len(cptid)=2 
--select CPTID from ##MitchellUrdSampleDataDR where CPTID like '%AGR'
--update ##MitchellUrdSampleDataDR set CPTID=REPLACE(CPTID,'[-]AGR','') where CPTID like '%AGR'
--update ##MitchellUrdSampleDataDR set CPTID = LEFT(CPTID,5) where CPTID like '%AGR'




--select  * from  ReviewStat.dbo.ETL_DETERMINATION_MAPPING where DeterminationID = '100000042'






--select CPTID,LEFT(CPTID,5) from ##MitchellUrdSampleDataDR where CPTID like '%-__'
----update ##MitchellUrdSampleDataDR set CPTID = LEFT(CPTID,5) where CPTID like '%-__'
--select CPTID,LEFT(CPTID,5) from ##MitchellUrdSampleDataDR where CPTID like '%-____'
--select CPTID from ##MitchellUrdSampleDataDR where len(CPTID)=3
----update ##MitchellUrdSampleDataDR set CPTID='00'+CPTID where len(CPTID)=3
--select CPTID from ##MitchellUrdSampleDataDR where len(CPTID)=4
----update ##MitchellUrdSampleDataDR set CPTID='0'+CPTID where len(CPTID)=4

--update ##MitchellUrdSampleDataDR set CPTID='S5000 ' where CPTID IN (SELECT CPTID FROM ##MISSING_CPTID)
--CREATE TABLE ##MISSING_CPTID (
--CPTID VARCHAR(50))
--CREATE TABLE ##CPT (
--CPTID VARCHAR(50))
--insert into ##cpt  select distinct cptid FROM rsimport..DetailedrequestLog(NOLOCK) where errormessage like '%CPT%' and filebatchid=846 
--select * from ##MitchellUrdSampleDataDR where cptid in (select cptid from ##cpt)

--====================================================================================================================
--- Final insert into detailedRequest table

--====================================================================================================================
insert into RSIMPORT.DBO.DetailedRequest(ReviewId,ExternalReviewId,Id,ExternalDetailedRequestId,DosStart,DosEnd,TreatmentId,SubTreatmentId,
CPTId,Units,AuthUnits,ICD9Id,DeterminationId,GuidelineId,Provider,
Cost,CostSavings,ProcessStatus,/*DetailedRequestId,CreateDateCst, */ClientId,FileBatchId,DescriptionText,BodyPartId,CptNdcModifier,AuthorizedCpt,
AuthorizedCptNdcModifier,ErrorMessage,Color,SideId)

select   '' as ReviewId,ExternalReviewId  ,Id, ExternalDetailedRequestId, DosStart, DosEnd, TreatmentId, SubTreatmentId,
CPTId, Units, AuthUnits, ICD9Id, DeterminationId, GuidelineId, Provider,
Cost, CostSavings, ProcessStatus,/*DetailedRequestId , CreateDateCst,*/ ClientId, FileBatchId, DescriptionText, BodyPartId, CptNdcModifier, AuthorizedCpt,
AuthorizedCptNdcModifier, ErrorMessage, Color, SideId from ##MitchellUrdSampleDataDR 
where ExternalReviewId in 
(Select ExternalReviewId from Reviewstat..Review (nolock) where ExternalReviewId is not null 
and RefLocationID='24262');


--and ExternalReviewId in (SELECT ExternalReviewId FROM DETAILEDREQUESTLOG (NOLOCK) WHERE FILEBATCHID=70634 AND ERRORMESSAGE !='OK' )

--Select ExternalReviewId from Reviewstat..Review (nolock) where ExternalReviewId is not null and RefLocationID='10902' and ExternalReviewId ='5585600'
-----TO IMPORT DR'S FOR WHICH REVIEWS ARE LOCKED
--select  DISTINCT '' as ReviewId,A.ExternalReviewId  ,A.Id, A.ExternalDetailedRequestId, A.DosStart, A.DosEnd, A.TreatmentId, A.SubTreatmentId,
null as CPTId, A.Units, A.AuthUnits, A.ICD9Id, A.DeterminationId, A.GuidelineId, A.Provider,
A.Cost, A.CostSavings, A.ProcessStatus,/*DetailedRequestId , CreateDateCst,*/ A.ClientId, A.FileBatchId, A.DescriptionText, A.BodyPartId, A.CptNdcModifier, A.AuthorizedCpt,
A.AuthorizedCptNdcModifier, A.ErrorMessage, A.Color, A.SideId from ##MitchellUrdSampleDataDR A
join DETAILEDREQUESTLOG d (NOLOCK)  on d.ExternalReviewId=A.ExternalReviewId and d.Units =A.Units and d.AuthUnits=A.AuthUnits
and A.DosStart=d.DosStart and  A.DosEnd=d.DosEnd
where A.ExternalReviewId in (SELECT ExternalReviewId FROM DETAILEDREQUESTLOG (NOLOCK) WHERE FILEBATCHID=70633 AND ERRORMESSAGE !='OK' ) --12

----where cptid in (select CPTID from  ##InvalidCPTID)

--where len(CPTID)<5

--delete  from RSIMPORT.DBO.DetailedRequest where filebatchid=1652
----where CPTID in (select CPTID from DETAILEDREQUESTlog(nolock) where filebatchid=1407 and errormessage !='OK' AND ERRORMESSAGE LIKE '%CPT%')
----where cptid in (select cptid from ##cpt) --used for loading cpt errored records only

--update ##MitchellUrdSampleDataDR set CPTID='S5000' where cptid in
--(select CPTID from DETAILEDREQUESTlog(nolock) where filebatchid=1409 and errormessage !='OK' AND ERRORMESSAGE LIKE '%CPT%' )and CPTID like '%-%'

