

with hrs as (select * from aamc.MinutesDimension  
where DATEPART(minute,TimeValue)=0  
),  
loc as (  
select loc.*, h.pat_id from aamc.AAMC_V_PAT_ADT_LOCATION_HX loc
join pat_Enc_hsp h on h.PAT_ENC_CSN_ID = loc.PAT_ENC_CSN_ID
where h.HOSP_ADMSN_TIME >='2-1-2020'
) 


,ob as (
/*just the deliveries in the last year*/
select * from aamc.delivery where DeliveryTime between dateadd(month, -12, getdate()-1) and getdate()-1 
), p as (
    /*this gets all encounters where there's a pregnancy dx, and if there's a delivery date after that encounter*/
    select  
        dx.PAT_ID
        ,dx.PAT_ENC_DATE_REAL
        ,dx.LINE
        ,dx.CONTACT_DATE
        ,dx.PAT_ENC_CSN_ID
        ,dx.DX_ID
        ,PregnancyDX.DX_NAME
        ,PregnancyDX.CURRENT_ICD10_LIST
        ,NextDelivery.DeliveryTime as NextDeliveryDate
        ,NextDelivery.MOM_CSN as NextDeliveryCSN
    from pat_enc_dx dx
    join (
        select 
        edg.DX_ID
        ,edg.DX_NAME
        ,edg.CURRENT_ICD10_LIST
        from clarity_edg edg where (edg.CURRENT_ICD10_LIST like 'O__.%' or edg.CURRENT_ICD10_LIST like '%Z3A.%' or edg.CURRENT_ICD10_LIST between 'Z32.%' and 'Z39.2')
    ) PregnancyDX on dx.DX_ID = PregnancyDX.DX_ID


    outer apply (
        select top 1 v.mom_id, v.mom_csn, v.DeliveryTime 
        from ob v
        where v.MOM_ID = dx.PAT_ID
        and cast(v.DeliveryTime as date) > dx.CONTACT_DATE /*delivery occurs after pregnancy diagnosis*/
        order by v.DeliveryTime desc
    )NextDelivery


    where dx.CONTACT_DATE between dateadd(month, -9, getdate()-1) and getdate()-1


), temp as (
    /*this gets just the patients, and if there's a delivery date*/
    select distinct p.pat_id, p.NextDeliveryDate, p.NextDeliveryCSN from p


), PregnantPatients as(
    /*all hospital encounters where a patient is pregnant*/
    select distinct 
        temp.NextDeliveryDate
        ,temp.NextDeliveryCSN
        ,eh.PAT_ID
        ,eh.PAT_ENC_CSN_ID
        ,eh.HOSP_ADMSN_TIME


    from pat_enc eh
    join temp on eh.PAT_ID = temp.PAT_ID
    where (eh.HOSP_ADMSN_TIME between dateadd(month, -9, getdate()-1) and getdate()-1)
    and
    (temp.NextDeliveryDate is null or eh.HOSP_ADMSN_TIME <= cast(temp.NextDeliveryDate as date)) /*no delivery date or admission time before the delivery date*/
    --and temp.NextDeliveryCSN != eh.PAT_ENC_CSN_ID /*uncomment this to exclude delivery encounters*/
)


,O as (  
select
op.proc_id  
,op.ORDER_PROC_ID  
,res.line  
,res.RESULT_TIME
,res.RESULT_DATE
,op.ORDER_TIME
,op.PAT_ID  
,case when res.ORD_VALUE = 'DETECTED' then 'POSITIVE' else 'NEGATIVE' end ORD_VALUE  
,ROW_NUMBER() over (partition by op.PAT_ID order by coalesce(res.ORD_VALUE,'ZZZ'), line desc ,res.RESULT_TIME desc, res.ORDER_PROC_ID desc) rn
,op.PAT_ENC_CSN_ID  
 ,dep.DEPARTMENT_NAME /*Pt Location at Order Time*/
from ORDER_PROC op  
join CLARITY_EAP eap on eap.proc_id = op.proc_id
left join ORDER_RESULTS res on op.order_proc_id = res.ORDER_PROC_ID and res.COMPONENT_ID in (1231000434
,1231000433
,1231000435
,1231000424
,1231000426
,1231000440
,1231000441
,1231000442
,1231000443
,1231000444
,1230400289
,1230500116 /*SARS-COV-2 RT-PCR-AAMC*/)  
left join ORDER_PROC_2 op2 on res.ORDER_PROC_ID=op2.ORDER_PROC_ID
left join clarity_dep dep on op2.PAT_LOC_ID=dep.DEPARTMENT_ID
where 1=1  
and eap.proc_code in ('LAB123232'
,'LAB123233'
,'LAB123234'
,'LAB123236'
,'LAB123237'
,'POC125')  
),

CovidCases as (  
select distinct  
o.ORDER_PROC_ID  
,o.PAT_ID  
,o.PAT_ENC_CSN_ID
,coalesce(res.RES_INST_ORDERED_TM,o.ORDER_TIME) SUSPECTED_DATETIME
,o.RESULT_TIME VERIFIED_DATETIME  
,o.ord_value  
from O
left join RES_DB_MAIN res on res.res_order_id = o.order_proc_id
where rn = 1
union  
select  distinct
null as ORDER_ID  
,dx.PAT_ID  
,enc.PAT_ENC_CSN_ID
,min(EFFECTIVE_DATE_DTTM) as SUSPECTED_DATETIME  
,min(case when dx.DX_ID in ('1494811646','1494811710') then enc.effective_date_dttm end) as VERIFIED_DATETIME 
,min(case dx.DX_ID 
    when '1494811646' then 'POSITIVE' 
    when '1494811710' then 'POSITIVE'
    when '1494811677' then 'SUSPECTED'
    end) as ord_value  
from pat_enc_dx dx  
join pat_enc enc on enc.pat_enc_csn_id = dx.pat_enc_csn_id
where dx.DX_ID in ('1494811646' , '1494811677', '1494811710' ) and dx.CONTACT_DATE >= '2-1-2020'
group by dx.PAT_ID,enc.PAT_ENC_CSN_ID
)

, dupCases as ( 
select md.CalculatedValue  
, pat.PAT_ID  
,FLOOR(DATEDIFF(DD,CAST(pat.BIRTH_DATE as date),coalesce(hsp.HOSP_ADMSN_TIME,cc.SUSPECTED_DATETIME))/365.242199) as PatientAge
, coalesce(loc.PAT_ENC_CSN_ID,cc.PAT_ENC_CSN_ID) LOC_CSN
, loc.PAT_ENC_CSN_ID ADMIT_CSN
, coalesce(hsp.HOSP_ADMSN_TIME,cc.SUSPECTED_DATETIME) EncounterTime
, cc.SUSPECTED_DATETIME SUSPECTED_TIME
, cc.VERIFIED_DATETIME VERIFIED_TIME
, ZC_PAT_CLASS.NAME ServiceLevel
, loc.EVENT_TYPE_C
, dd.NAME DischargeDispositon
, hsp.HOSP_DISCH_TIME
, pat.pat_name 
, ORDER_PROC_ID
, case when FLOOR(DATEDIFF(DD,CAST(pat.BIRTH_DATE as date),coalesce(hsp.HOSP_ADMSN_TIME,cc.SUSPECTED_DATETIME))/365.242199) < 18 then 1 else 0 end as IsPEDs
, case when ORDER_PROC_ID is null then 'NO' else 'YES' end InternalTestStatus
, loc.ROOM_NUMBER
, ROW_NUMBER() over (partition by pat.PAT_ID,CalculatedValue order by coalesce(loc.IN_DTTM,0) desc, coalesce(order_proc_id,0) desc, SUSPECTED_DATETIME desc) rn 
, case when CalculatedValue>=hsp.HOSP_DISCH_TIME 
      then 'Post ' + ZC_PAT_CLASS.NAME + ' Discharge' 
    else coalesce(case loc.EVENT_TYPE_C when 2 then 'Post ' + ZC_PAT_CLASS.NAME + ' Discharge'  end, loc.ADT_DEPARTMENT_NAME,'Not Admitted') 
    end ADT_DEPARTMENT_NAME  
,case
when ORDER_PROC_ID is null then cc.ORD_VALUE 
when md.CalculatedValue>cc.Verified_Datetime then cc.ORD_VALUE  
when md.CalculatedValue between cc.SUSPECTED_DATETIME and cc.Verified_Datetime or cc.Verified_Datetime is null then 'IN PROGRESS'  
end OrderStatus
, case when VENT_DAY_BOOL = 1 then 'Yes' else 'No' end VentDay
, case when ve.PAT_ID is not null then 'Yes' else 'No' end ActiveVent
, case when dep.Specialty='Critical Care Medicine' and loc.Event_type_c !=2 then 'Yes' else 'No' end ICUStatus
from  
CovidCases cc  
join hrs md on md.CalculatedValue>='2-1-2020' and md.CalculatedValue <=GETDATE() and md.CalculatedValue >=cc.SUSPECTED_DATETIME
left join loc on cc.PAT_ID = loc.pat_id and md.CalculatedValue between loc.IN_DTTM and loc.OUT_DTTM  
left join ZC_PAT_CLASS on ZC_PAT_CLASS.INTERNAL_ID = loc.PAT_CLASS_C
left join PAT_ENC_HSP hsp on hsp.pat_enc_csn_id = coalesce(loc.pat_enc_csn_id, cc.pat_enc_csn_id)
left join ZC_DISCH_DISP dd on dd.INTERNAL_ID = hsp.DISCH_DISP_C 
left join F_IP_HSP_PAT_DAYS vent on VENT_DAY_BOOL=1 and vent.PAT_ID = hsp.PAT_ID and vent.CALENDAR_DT = md.DateValue
left join F_VENT_EPISODES ve on ve.PAT_ID = cc.PAT_ID and ve.VENT_START_DTTM <= md.CalculatedValue and (ve.VENT_END_DTTM is null or ve.VENT_END_DTTM >=md.CalculatedValue)
left join CLARITY_DEP dep on dep.Department_id = loc.ADT_DEPARTMENT_ID
join patient pat on pat.pat_id = cc.pat_id  
) 


select cast(CensusHour as date) as CensusDate, OrderStatus, max(OrderStatusCount) as OrderStatusCount from (
select CensusHour, OrderStatus, count(*) OrderStatusCount from (
select distinct
  dupCases.CalculatedValue as CensusHour
, dupCases.OrderStatus
, dupCases.PAT_ID
--, count(*) OrderStatusCount, dupCases.PAT_NAME, dupCases.ADT_DEPARTMENT_NAME, dupCases.HOSP_DISCH_TIME, dupCases.DischargeDispositon
--,case when ob.PAT_ENC_CSN_ID is not null then 1 else 0 end as IsPregnant
--,cast (ob.NextDeliveryDate as date) as DeliveryDate
from dupCases
where ADT_DEPARTMENT_NAME <> 'Not Admitted'
  and ADT_DEPARTMENT_NAME not like 'Post % Discharge'
--  'Post Outpatient Discharge', 'Not Admitted', 'Post Emergency Discharge', 'Post Inpatient Discharge'
  and dupCases.OrderStatus = 'POSITIVE'
  --and dupCases.CalculatedValue < '2020-04-09 09:01'
--group by dupCases.CalculatedValue, dupCases.OrderStatus
) xxx
group by CensusHour, OrderStatus
) yyy
group by cast(CensusHour as date), OrderStatus
order by cast(CensusHour as date), OrderStatus
