--select sysdatetime(); 

/*
--drop table DBOps.DBO.Notes_GB
select * from DBOps.DBO.Notes_IME_Cal
547pc-1571619

*/

	

	----- STEP 2 ----- INSERT FLIEBATCH ID INTO FILEBATCH TABLE FOR FURTHER INSERTS-----------

insert into  FileBatch (InputId, FileName, InProcess, ReceivedDate, CompleteDate, ClientId, BatchAckDirectoryFolder, IsBulkLoad)
		 --values ('2029', 'Notes_IME_Cal', 0 ,	getdate(),	NULL	,'547',	NULL, 1 ) --FOR UAT
		  values 	('21',	'Notes_IME_Cal' ,  0 ,	getdate(),	NULL	,'545',	NULL, 1 ) --FOR PROD
			 

---------- STEP 3  -----------------------------------

declare @filebatchid int 
select @filebatchid= max(filebatchid) from FileBatch where ClientId = '545' AND FILENAME LIKE '%Notes_IME_Cal%'
--select @filebatchid

select 
IMEID as ReviewId,
IMEID as ExternalReviewId,

----LTRIM(RTRIM(claim_no)) as ClaimNumber, ---- COMMENTED BECAUSE EITHER CLAIMNUMBER OR EXTERNALCLAIMNUMBER IS TO BE PASSED
--claim_no as ClaimNumber,
--claim_id as ClaimNumber,

--LTRIM(RTRIM(claim_id)) as ExternalClaimNumber,

--CASE when len(LTRIM(RTRIM(claim_id))) =7 then '0000000000000' + LTRIM(RTRIM(claim_id))
	-- when len(LTRIM(RTRIM(claim_id))) =6 then '00000000000000' + LTRIM(RTRIM(claim_id)) 
	 --when len(LTRIM(RTRIM(claim_id))) =5 then '000000000000000'+ LTRIM(RTRIM(claim_id))  else LTRIM(RTRIM(claim_id)) end as ExternalClaimNumber,
 
ExternalClaimNumber as ExternalClaimNumber,
--CASE WHEN second_ID like 'FP1%' THEN
--LEFT(RTRIM(second_ID),3)+ '-'+ Substring(RTRIM(second_ID), 4, 4) + '-' + CONVERT(varchar,CAST(SUBSTRING(RTRIM(second_ID), 8, LEN(RTRIM(second_ID))-7) AS INT)) 
--ELSE LTRIM(RTRIM(second_id)) END AS ExternalClaimNumber,

'17' as NoteTypeId,
null as ActivityCodeId,
----time_posted as ActivityCodeId,
null as ResultCodeId,
date as NoteCreateDate,-- 
----text_note as Notes,-- 
--convert(varchar(30),time_posted )+'   '+ cast(text_note as varchar(MAX)) as Notes,
'Requesting Provider :'+ cast(text_note as varchar(MAX)) as Notes,

--'Time_Posted'+ ': '+ convert(varchar(30),time_posted )+'   '+ cast(text_note as varchar(MAX)) as Notes, --(change done to append note date and note )
null  as ContactDate,
null as ContactTime,
null as ContactTimeAMPM,
null as ContactTypeId,-- 
null as ContactPerson,-- 
null as ContactRole,
null as ExternalNoteId,
'' as ProcessStatus,
--NotesId,
'' as Seq,
date as CreateDateCst,
'544' as ClientId,-- 
null as LocationId,
null as ServiceType,
null as ReferralService,
null as CaseService,
null as Hours,
null as Expenses,
null as Description,
@filebatchid as FileBatchId, -- 
null as ErrorMessage,
null as NoteKind --

--CASE WHEN second_ID like 'LWC%' THEN
--LEFT(RTRIM(second_ID),3)+ '-'+ Substring(RTRIM(second_ID), 4, 4) + '-' + CONVERT(varchar,CAST(SUBSTRING(RTRIM(second_ID), 8, LEN(RTRIM(second_ID))-7) AS INT)) 
--ELSE LTRIM(RTRIM(second_id)) END as SmartAdvisorClaimMicId,

--second_ID as SmartAdvisorClaimMicId,
--Note_ID

 into ##DiaryNotesSampleDataReplica
from DBOps.DBO.Notes_IME_Cal

--select 'Time_Posted'+ ': '+ convert(varchar(30),time_posted )+'   '+ cast(text_note as varchar(MAX)) as Notes from DBOps.DBO.Notes_GB

--select  convert(varchar(30),time_posted )+'   '+ cast(text_note as varchar(MAX)) as Notes from DBOps.DBO.Notes_GB



------	select top 100 * from ##DiaryNotesSampleData
--DROP TABLE ##DiaryNotesSampleDataReplica
--select  * from ##DiaryNotesSampleDataReplica
--select distinct ContactTypeId from ##DiaryNotesSampleDataReplica
--select *    from   Noteslog where Filebatchid ='825'
--select Notes,* from ##DiaryNotesSampleDataReplica where cast(notes as varchar) =''
--select COUNT(*) from ##DiaryNotesSampleDataReplica --73934
--SELECT ExternalClaimNumber,'0000000000000'+DN.ExternalClaimNumber FROM ##DiaryNotesSampleDataReplica DN

--------------------- STEP 4

--select * from RSIMPORT.DBO.Notes where ClaimNumber is NOT NULL;
--alter table RSIMPORT.DBO.Notes
--alter column ClaimNumber varchar(50) null;

--alter table RSIMPORT.DBO.Notes
--alter column ExternalClaimNumber varchar(50) null;


Insert into RSIMPORT.DBO.Notes
(ReviewId,ExternalReviewId, ClaimNumber,
ExternalClaimNumber,NoteTypeId,ActivityCodeId,ResultCodeId,
NoteCreateDate,Notes,
ContactDate,ContactTime,
ContactTimeAMPM,ContactTypeId,ContactPerson,ContactRole,ExternalNoteId,ProcessStatus,/* --NotesId,
Seq, */CreateDateCst,ClientId,LocationId,ServiceType,ReferralService,CaseService,Hours,Expenses,Description,FileBatchId,ErrorMessage,NoteKind,
SmartAdvisorClaimMicId)
--cn.ClientId as clientNew
select distinct
ReviewId, ExternalReviewId,  '' as ClaimNumber, '' as  ExternalClaimNumber,
--'0000000000000'+DN.ExternalClaimNumber, ----DN.ExternalClaimNumber,

NoteTypeId,ActivityCodeId,ResultCodeId,
convert(datetime,cast (NoteCreateDate as varchar )) as NoteCreateDate,Notes,convert(datetime,cast ( ContactDate as varchar)) as ContactDate,ContactTime,
ContactTimeAMPM, null as  ContactTypeId,ContactPerson,ContactRole,ExternalNoteId,'0' as ProcessStatus , 
 CAST(NoteCreateDate as datetime) as CreateDateCst,
'545' as ClientId, '24262' as LocationId, ----'1' as LocationId,
ServiceType,null as ReferralService,null as CaseService,Hours,Expenses,
Description, FileBatchId as FileBatchId,
ErrorMessage,null as NoteKind,
null as SmartAdvisorClaimMicId

----,cn.SmartAdvisorMicId,DN.ClaimNumber , CN.ClaimNumber --(for validation of data only)
from ##DiaryNotesSampleDataReplica DN inner join reviewstat.dbo.ClaimNumber CN on
--'0000000000000' + DN.ExternalClaimNumber = CN.ExternalClaimNumber --13096DN.ClaimNumber = CN.ClaimNumber AND CN.LocationId='10902'
DN.ExternalClaimNumber = CN.ExternalClaimNumber
--AND SmartAdvisorClaimMicId=SmartAdvisorMicId  --398
and cn.clientid=545 -- 

select ExternalClaimNumber,ClaimNumber from ReviewStat.dbo.ClaimNumber where ClaimNumber = '303113'
