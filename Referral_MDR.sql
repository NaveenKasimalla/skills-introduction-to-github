
--select ExternalClaimNumber, M.Claimid, Claimnumber from DBOps.DBO.MDR_Cal M ,RSImport.Dbo.Claim c where M.ClaimID = c.ClaimNumber

--select * from DBOps.DBO.MDR_Cal_1 M
--select * from DBOps.DBO.RMD_Cal

declare @fileName varchar (100)

set @fileName= 'RMD_Cal' 

insert into  FileBatch (InputId, FileName, InProcess, ReceivedDate, CompleteDate, ClientId, BatchAckDirectoryFolder, IsBulkLoad)
				
			--values 	('2029', 'Referral_MIC', 0 ,	getdate(),	NULL	,'545',	NULL, 1 ) --FOR UAT
			values 	('21', 'RMD_Cal'	,  0 ,	getdate(),	NULL	,'545',	NULL, 1 ) --FOR PROD

--================================================================================================================================================
--select * from Referral
--STEP 1 -- INSERT DATA INTO ANOTHER TEMPORARY TABLE
--================================================================================================================================================

declare @filebatchid int 
select @filebatchid= max(filebatchid) from FileBatch where ClientId = '545' AND filename like '%RMD_Cal%'
--select @filebatchid

select distinct
null as ReviewId, -- not to be passed on import 
PrecertID as ReferralPerson, -- Hard code value history record
'24262' as LocationId,			
null as AccountNumber,
null as GroupId,
'113' as CaseServiceTypeId, -- Default to UR..need valid id  value
'119' as ServiceTypeId, -- Will be supplied by ReviewStat ID values sent by Dennis
'1' as ReviewLevelId, -- Will be supplied by ReviewStat ID values sent by Dennis
'269' as ReviewTypeId, -- Will be supplied by ReviewStat ID values sent by Dennis

-- WHEN ReviewTypeId = '269',
     
'' as MatrixId, ---- Will be supplied by ReviewStat ID values sent by Dennis
null as BPartId,
null as ConditionId,
null as ExternalCaseContactId,
null as Nurse,
null as ExternalCaseAdminId,
null as CaseAdminId,
null as CarrierReceivedDate,
MDRDate as ReceivedDate, -- current null, needs to be updated after history migration, currently mapped to start date
null as CaseCreateDate,
null as DocRecDate,
null as UMDReferralDate,
null as ClientDueDate,
null as PAAssignDate,
null as PADue,
null as InternalDueDate,
null as JurisdictionDueDate,
--LTRIM(RTRIM(service_code)) as CPT,
null as ICD9,
TreatmentReason as Request,
null as Diag,
null as Frequency,
null as Length,
null as FacilityContactFName,
null as FacilityContactLName,
null as FacilityContactPhone,
null as FacilityName,
null as FacilityTaxId,
null as FacilityAddress1,
null as FacilityAddress2,
null as FacilityCity,
null as FacilityState,
null as FacilityZip,
null as FacilityPhone,
null as FacilityExtension,
null as FacilityFax,
null as FacilityExtId,
null as FacilityEmail,
null as FacilityWebsite,
null as ReqPhysicianId,
null as ReqPhysicianFName,
null as ReqPhysicianLName,
null as ReqPhysicianMName,
null as ReqPhysicianLicenseType,
null as ReqPhysicianSpecialty,
null as ReqPhysicianAddress1,
null as ReqPhysicianAddress2,
null as ReqPhysicianCity,
null as ReqPhysicianState,
null as ReqPhysicianZip,
null as ReqPhysicianPhone,
null as ReqPhysicianFax,
null as ReqPhysicianExtension,
null as ReqPhysicianCell,
null as ReqPhysicianContactName,
null as ReqPhysicianEmail,
null as ReqPhysicianContactFName,
null as ReqPhysicianContactLName,
null as ReqPhysicianContactPhone,
null as TrPhysicianId,
null as TrPhysicianLName,
null as TrPhysicianFName,
null as TrPhysicianMName,
null as TrPhysicianLicenseType,
null as TrPhysicianSpecialty,
null as TrPhysicianAddress1,
null as TrPhysicianAddress2,
null as TrPhysicianCity,
null as TrPhysicianState,
null as TrPhysicianZip,
null as TrPhysicianPhone,
null as TrPhysicianFax,
null as TrPhysicianExtension,
null as TrPhysicianCell,
null as TrPhysicianContact,
null as TrPhysicianEmail,
null as TrPhysicianContactFName,
null as TrPhysicianContactLName,
null as TrPhysicianContactPhone,
'0' as ProcessStatus,
LTRIM(RTRIM(MDRID)) as ExternalReviewId ,

LTRIM(RTRIM(RS_ClaimNumbers)) as ExternalClaimNumber,

--LTRIM(RTRIM(ExternalClaimNumber)) as ExternalClaimNumber,

--CASE when len(LTRIM(RTRIM(claim_id))) =7 then '0000000000000' + LTRIM(RTRIM(claim_id))
	 --when len(LTRIM(RTRIM(claim_id))) =6 then '00000000000000' + LTRIM(RTRIM(claim_id)) 
	 --when len(LTRIM(RTRIM(claim_id))) =5 then '000000000000000'+ LTRIM(RTRIM(claim_id))  else LTRIM(RTRIM(claim_id)) end as ExternalClaimNumber,
	
----CASE WHEN second_ID like 'FP1%' THEN
----LEFT(RTRIM(second_ID),3)+ '-'+ Substring(RTRIM(second_ID), 4, 4) + '-' + CONVERT(varchar,CAST(SUBSTRING(RTRIM(second_ID), 8, LEN(RTRIM(second_ID))-7) AS INT)) 
----ELSE LTRIM(RTRIM(second_id)) END AS ExternalClaimNumber,

null as ReferralId, ----nonclustered, unique, unique key located on PRIMARY
null as Seq, --clustered, unique, primary key located on PRIMARY
null as CreateDateCst,
'545'as clientID,  
@filebatchid as FileBatchId, --- will get filebatch id from batch_id from FileBatch table
null as IsRushRequested, --"N"
null as ExternalReqPhysicianId,
null as ReqPhysicianClinic,
null as ExternalTrPhysicianId,
null as ErrorMessage,
null as TrPhysicianClinic,
'545' as  [Status], --- '521', '81'   as  [Status], -- Need default status , update by dennis on 14 june 18
null as UseExistingFacility,
null as UseExistingReqPhysician,
null as UseExistingTrPhysician,
null as ExternalFacilityAddressId,
null as ExternalReqPhysicianAddressId,
null as ExternalTrPhysicianAddressId
----row_number () over (order by record_type) as rownum --- pseudo column for rownumber only
into   ##MitchellUrdSampleDataReplica --9926

from DBOps.dbo.RMD_Cal;

--================================================================================================================================================
 
 --drop table DBOps.dbo.IME_Cal_1;

 select * from DBOps.dbo.RMD_Cal;;
 /*
 
 SELECT DISTINCT ExternalReviewId FROM  ##MitchellUrdSampleDataReplica --9186
 SELECT ExternalReviewId ,* FROM  ##MitchellUrdSampleDataReplica ORDER BY 1
 select * from ##MitchellUrdSampleDataReplica
drop table ##MitchellUrdSampleDataReplica

select distinct ExternalReviewId from  ##MitchellUrdSampleDataReplica  --965
 select FileBatchId,InputId,FileName,InProcess,ReceivedDate,CompleteDate,ClientId,BatchAckDirectoryFolder from FileBatch
 */

--STEP 2
--================================================================================================================================================
-- select  *  from ##MitchellUrdSampleDataReplica 
-- select *  From ##MitchellUrdSampleData ;
/*
Not in use 
update  ##MitchellUrdSampleDataReplica set CaseServiceTypeId  = '2'  where  rownum%2 =0
update  ##MitchellUrdSampleDataReplica set MatrixId    = '2' where  rownum%2 =0
update ##MitchellUrdSampleDataReplica set ServiceTypeId   = '2'  where  rownum%2 =0
update ##MitchellUrdSampleDataReplica set ReviewLevelId   = '1' where  rownum%2 =0
update ##MitchellUrdSampleDataReplica set ReviewTypeId   = '2'  where  rownum%2 =0
update  ##MitchellUrdSampleDataReplica  set MatrixId   = '1' where  rownum%2 =0


*/

--================================================================================================================================================

--STEP 3 -- INSERT DATA INTO ANOTHER TEMPORARY TABLE
--================================================================================================================================================
insert into  RSIMPORT.DBO.Referral (ReviewId,ReferralPerson,LocationId,AccountNumber,GroupId,CaseServiceTypeId,ServiceTypeId,ReviewLevelId,ReviewTypeId,MatrixId,BPartId,ConditionId,
ExternalCaseContactId,Nurse,ExternalCaseAdminId,CaseAdminId,CarrierReceivedDate,ReceivedDate,CaseCreateDate,DocRecDate,UMDReferralDate,ClientDueDate,PAAssignDate,
PADue,InternalDueDate,JurisdictionDueDate,CPT,ICD9,Request,Diag,Frequency,Length,FacilityContactFName,FacilityContactLName,FacilityContactPhone,FacilityName,FacilityTaxId,
FacilityAddress1,FacilityAddress2,FacilityCity,FacilityState,FacilityZip,FacilityPhone,FacilityExtension,FacilityFax,FacilityExtId,FacilityEmail,FacilityWebsite,ReqPhysicianId,ReqPhysicianFName,
ReqPhysicianLName,ReqPhysicianMName,ReqPhysicianLicenseType,ReqPhysicianSpecialty,ReqPhysicianAddress1,ReqPhysicianAddress2,ReqPhysicianCity,ReqPhysicianState,ReqPhysicianZip,
ReqPhysicianPhone,ReqPhysicianFax,ReqPhysicianExtension,ReqPhysicianCell,ReqPhysicianContactName,ReqPhysicianEmail,ReqPhysicianContactFName,ReqPhysicianContactLName,ReqPhysicianContactPhone,
TrPhysicianId,TrPhysicianLName,TrPhysicianFName,TrPhysicianMName,TrPhysicianLicenseType,TrPhysicianSpecialty,TrPhysicianAddress1,TrPhysicianAddress2,TrPhysicianCity,TrPhysicianState,
TrPhysicianZip,TrPhysicianPhone,TrPhysicianFax,TrPhysicianExtension,TrPhysicianCell,TrPhysicianContact,TrPhysicianEmail,TrPhysicianContactFName,TrPhysicianContactLName,TrPhysicianContactPhone,
ProcessStatus,ExternalReviewId,ExternalClaimNumber,CreateDateCst,ClientId,FileBatchId,IsRushRequested,ExternalReqPhysicianId,ReqPhysicianClinic,ExternalTrPhysicianId,
ErrorMessage,TrPhysicianClinic,Status,UseExistingFacility,UseExistingReqPhysician,UseExistingTrPhysician,ExternalFacilityAddressId,ExternalReqPhysicianAddressId,ExternalTrPhysicianAddressId) 

select distinct ReviewId,ReferralPerson,URD.LocationId,AccountNumber,GroupId,CaseServiceTypeId,ServiceTypeId,ReviewLevelId,ReviewTypeId,MatrixId,BPartId,ConditionId,
ExternalCaseContactId,Nurse,ExternalCaseAdminId,CaseAdminId,CarrierReceivedDate,ReceivedDate,CaseCreateDate,DocRecDate,UMDReferralDate,ClientDueDate,PAAssignDate,
PADue,InternalDueDate,JurisdictionDueDate, null  AS CPT,
--(select MAX(CPT) from ##MitchellUrdSampleDataReplica where ExternalReviewId= URD.ExternalReviewId), 
ICD9,Request,Diag,Frequency,Length,FacilityContactFName,FacilityContactLName,FacilityContactPhone,FacilityName,FacilityTaxId,
FacilityAddress1,FacilityAddress2,FacilityCity,FacilityState,FacilityZip,FacilityPhone,FacilityExtension,FacilityFax,FacilityExtId,FacilityEmail,FacilityWebsite,ReqPhysicianId,ReqPhysicianFName,
ReqPhysicianLName,ReqPhysicianMName,ReqPhysicianLicenseType,ReqPhysicianSpecialty,ReqPhysicianAddress1,ReqPhysicianAddress2,ReqPhysicianCity,ReqPhysicianState,ReqPhysicianZip,
ReqPhysicianPhone,ReqPhysicianFax,ReqPhysicianExtension,ReqPhysicianCell,ReqPhysicianContactName,ReqPhysicianEmail,ReqPhysicianContactFName,ReqPhysicianContactLName,ReqPhysicianContactPhone,
TrPhysicianId,TrPhysicianLName,TrPhysicianFName,TrPhysicianMName,TrPhysicianLicenseType,TrPhysicianSpecialty,TrPhysicianAddress1,TrPhysicianAddress2,TrPhysicianCity,TrPhysicianState,
TrPhysicianZip,TrPhysicianPhone,TrPhysicianFax,TrPhysicianExtension,TrPhysicianCell,TrPhysicianContact,TrPhysicianEmail,TrPhysicianContactFName,TrPhysicianContactLName,TrPhysicianContactPhone,
ProcessStatus,ExternalReviewId, 
----case when len(URD.ExternalClaimNumber) <10 then '0000000000000' +URD.ExternalClaimNumber else URD.ExternalClaimNumber end ,
URD.ExternalClaimNumber,   --, CN.ExternalClaimNumber,CN.LOCATIONID,
getdate() as CreateDateCst,URD.clientID,FileBatchId,IsRushRequested,ExternalReqPhysicianId,ReqPhysicianClinic,ExternalTrPhysicianId,
ErrorMessage,TrPhysicianClinic,Status,UseExistingFacility,UseExistingReqPhysician,UseExistingTrPhysician,ExternalFacilityAddressId,ExternalReqPhysicianAddressId,ExternalTrPhysicianAddressId
from ##MitchellUrdSampleDataReplica  URD inner join reviewstat.dbo.ClaimNumber CN 
on  URD.ExternalClaimNumber = CN.ExternalClaimNumber 
--on  URD.ExternalClaimNumber = CN.ClaimNumber 
AND URD.LocationId=CN.LocationId ;




--where ExternalReviewId ='79236'
----'0000000000000' + URD.ExternalClaimNumber = CN.ExternalClaimNumber 
--where ExternalReviewId not in (select ExternalReviewId from ##A) --

-- AND  URD.ExternalClaimNumber LIKE 'FP%'  --and ExternalReviewId >=900020 --129120
--where ExternalReviewId in (select ExternalReviewId from REviewStat..Review (nolock) reflocationid='1000000193' ) --2772
--33705--32556

--(select distinct ExternalReviewId into ##A from REviewStat..Review (nolock) where reflocationid='1000000193' and ExternalReviewId is not null )
--select distinct ExternalReviewId from ##MitchellUrdSampleDataReplica
--create clustered select top 1 * from referrallog

70258
