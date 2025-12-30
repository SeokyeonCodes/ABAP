"! <p class="shorttext synchronized" lang="en">AMDP Class</p>
CLASS ztm_cl_amdp DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES if_amdp_marker_hdb.


    "! <p class="shorttext synchronized" lang="en">출항 Delay List</p>
    CLASS-METHODS calc_delay_days FOR TABLE FUNCTION ztmfsdd2300_ft.

    "! <p class="shorttext synchronized" lang="en">서류 마감 현황 & 특이건</p>
    CLASS-METHODS: get_data FOR TABLE FUNCTION ztmfsdd2400_tf.

    "! <p class="shorttext synchronized" lang="en">FWO 현황(P&E)</p>
    CLASS-METHODS: fwo_worklist_pne FOR TABLE FUNCTION ztmfsdd2900_tf.

    "! <p class="shorttext synchronized" lang="en">FWO 현황(Created)</p>
    CLASS-METHODS: fwo_worklist_created FOR TABLE FUNCTION ztmfsdd2910_tf.

    "! <p class="shorttext synchronized" lang="en">FWO 현황(Team)</p>
    CLASS-METHODS: fwo_worklist_team FOR TABLE FUNCTION ztmfsdd2920_tf.

    "! <p class="shorttext synchronized" lang="en">거래명세서 리스트(User & Team)</p>
    CLASS-METHODS get_iss_list_user_n_team FOR TABLE FUNCTION ztmfsdd3100_tf.

    "! <p class="shorttext synchronized" lang="en">MPA DEMURRAGE</p>
    CLASS-METHODS mpa_demurrage FOR TABLE FUNCTION ztmfsdd3300_tf.

    "! <p class="shorttext synchronized" lang="en">MPA DETENTION</p>
    CLASS-METHODS mpa_detention FOR TABLE FUNCTION ztmfsdd3310_tf.

  PROTECTED SECTION.

  PRIVATE SECTION.

ENDCLASS.



CLASS ZTM_CL_AMDP IMPLEMENTATION.


  METHOD calc_delay_days
    BY DATABASE FUNCTION FOR HDB
    LANGUAGE SQLSCRIPT
    OPTIONS READ-ONLY
    USING /scmtms/d_torrot /scmtms/d_torstp /scmtms/d_torexe /sapapo/loc.
*동성 협력사 kpi 앱타일 추가 요건 때문에, 해당 method 수정 d230182 2023.11.20
    declare lv_query string;
    declare lv_condition string;
**    lt_tzone =
**        select TOP 1
**         tzone
**         from "/SAPAPO/LOC"
**         where locno like 'KR%'
**         ;

        lt_data =
            select
             _root.mandt as client,
             _root.tor_id,
             _root.resp_person,
             _root.created_by,
*         localtoutc(  to_timestamp( _stop.plan_trans_time), _loc.tzone ) as test_localtoutc,
*         utctolocal(  to_timestamp( _stop.plan_trans_time), _loc.tzone ) as test_utctolocal,
             to_dats(utctolocal((to_timestamp( _stop.plan_trans_time ) ), _loc.tzone ) ) as etd_d,
             to_dats(utctolocal((to_timestamp( _exec.actual_date ) ), _loc.tzone) ) as atd_d,
*         rank ( ) over ( partition by to_dats(utctolocal((to_timestamp( _exec.actual_date ) ), _loc.tzone) )
*                         order     by _exec.actual_date ) as rank,
*         RANK ( ) OVER ( partition by _exec.created_on order by _exec.created_on ) as rank
              ROW_NUMBER ( ) OVER( PARTITION by _root.mandt, tor_id, resp_person order by tor_id ) as row_num
             from "/SCMTMS/D_TORROT"             as _root
             inner join "/SCMTMS/D_TORSTP"       as _stop on _stop.parent_key = _root.db_key
                                                         and _stop.stop_role  = 'MS' -- Main stage
                                                         and _stop.stop_cat   = 'O'  -- outbound
                                                         and _stop.mandt      = _root.mandt
             inner join "/SAPAPO/LOC"            as _loc  on _loc.mandt       = _root.mandt
                                                         and _loc.locno       = _stop.log_locid
                                                         and _loc.loc_uuid    = _stop.log_loc_uuid
             left outer join "/SCMTMS/D_TOREXE"  as _exec on _exec.torstopuuid = _stop.db_key
                                                         and _exec.trans_activity = '03' -- departure
                                                         and _exec.event_revoked = ''
                                                         and _exec.mandt      = _root.mandt
*                                                     and _exec.actual_date = ( select min(actual_date) from "/SCMTMS/D_TOREXE"
*                                                                                                       WHERE torstopuuid    = _stop.db_key
*                                                                                                       and   trans_activity = '03' -- departure
*                                                                                                       and   event_revoked  = ''
*                                                                                                       and   mandt          = _root.mandt )

             where
             _root.mandt = session_context( 'CLIENT' )
* and _root.resp_person = p_user
             and _root.tor_type   <> 'ZBB0'
             and _root.lifecycle  <> '10'
             and _root.tor_cat         = 'BO' -- freight booking
             and _root.traffic_direct  = '1' -- EXPORT
             and _root.trmodcat        = '3' -- sea
             and _stop.log_locid       like 'KR%'
*         and localtoutc(  to_timestamp( _stop.plan_trans_time), _loc.tzone )
             and to_dats( utctolocal(  to_timestamp( _stop.plan_trans_time), _loc.tzone ) )
*                                                                                  --   etd -7 ~  etd +7
                                                                                  between to_dats(add_days( current_date, -7 ) ) and to_dats(add_days( current_date, +1 ) );
*            between to_timestamp(add_days( current_timestamp, -10 ) ) and to_timestamp(add_days( current_timestamp, +7 ) )
*         and to_timestamp(add_days( current_timestamp, -365 ) ) <= localtoutc( to_timestamp( _stop.plan_trans_time ), _loc.tzone )
*         and to_timestamp(add_days( current_timestamp, 7 ) ) >= localtoutc( to_timestamp( _stop.plan_trans_time ), _loc.tzone )

*           사용자 현글인 경우
            if p_account = 'H' then
                lt_data2 =
                    select
                        client,
                        days_between( etd_d, atd_d ) as delay_day,
                        CASE when atd_d = '00000000' then 'X' END as flag  --not departed
                     FROM :lt_data
                    WHERE row_num = 1
                      AND resp_person = p_user;

*          사용자 협력사인 경우 (동성, role: ZTM_GLE_OPERATOR_E701)
            ELSEIF p_account = 'P' THEN
                lt_data2 =
                    SELECT
                        client,
                        days_between( etd_d, atd_d ) as delay_day,
                        CASE when atd_d = '00000000' then 'X' END as flag  --not departed
                    FROM :lt_data
                   WHERE row_num = 1
                     AND created_by = p_user;
            END if;
*            RETURN SELECT
*                client,
**            resp_person,
*                count( CASE when flag = 'X'      then 1 END ) as not_departed,
*                count( CASE when delay_day = 1   then 1 END ) as delayed,
*                count( CASE when delay_day >= 2  then 1 END ) as delayed_2
*            from :lt_data2
*            GROUP BY client, resp_person; --, tor_id;

            RETURN SELECT client,
                    COUNT( CASE when flag = 'X'      THEN 1 END ) AS not_departed,
                    COUNT( CASE when delay_day = 1   THEN 1 END ) AS delayed,
                    COUNT( CASE when delay_day >= 2  THEN 1 END ) AS delayed_2
                   FROM :lt_data2
                   GROUP BY client;

***    lt_tzone =
***        select TOP 1
***         tzone
***         from "/SAPAPO/LOC"
***         where locno like 'KR%'
***         ;
*
*    lt_data =
*        select
*         _root.mandt as client,
*         _root.tor_id,
*         _root.resp_person,
**         localtoutc(  to_timestamp( _stop.plan_trans_time), _loc.tzone ) as test_localtoutc,
**         utctolocal(  to_timestamp( _stop.plan_trans_time), _loc.tzone ) as test_utctolocal,
*         to_dats(utctolocal((to_timestamp( _stop.plan_trans_time ) ), _loc.tzone ) ) as etd_d,
*         to_dats(utctolocal((to_timestamp( _exec.actual_date ) ), _loc.tzone) ) as atd_d,
**         rank ( ) over ( partition by to_dats(utctolocal((to_timestamp( _exec.actual_date ) ), _loc.tzone) )
**                         order     by _exec.actual_date ) as rank,
**         RANK ( ) OVER ( partition by _exec.created_on order by _exec.created_on ) as rank
*          ROW_NUMBER ( ) OVER( PARTITION by _root.mandt, tor_id, resp_person order by tor_id ) as row_num
*         from "/SCMTMS/D_TORROT"             as _root
*         inner join "/SCMTMS/D_TORSTP"       as _stop on _stop.parent_key = _root.db_key
*                                                     and _stop.stop_role  = 'MS' -- Main stage
*                                                     and _stop.stop_cat   = 'O'  -- outbound
*                                                     and _stop.mandt      = _root.mandt
*         inner join "/SAPAPO/LOC"            as _loc  on _loc.mandt       = _root.mandt
*                                                     and _loc.locno       = _stop.log_locid
*                                                     and _loc.loc_uuid    = _stop.log_loc_uuid
*         left outer join "/SCMTMS/D_TOREXE"  as _exec on _exec.torstopuuid = _stop.db_key
*                                                     and _exec.trans_activity = '03' -- departure
*                                                     and _exec.event_revoked = ''
*                                                     and _exec.mandt      = _root.mandt
**                                                     and _exec.actual_date = ( select min(actual_date) from "/SCMTMS/D_TOREXE"
**                                                                                                       WHERE torstopuuid    = _stop.db_key
**                                                                                                       and   trans_activity = '03' -- departure
**                                                                                                       and   event_revoked  = ''
**                                                                                                       and   mandt          = _root.mandt )
*
*         where
*         _root.mandt = session_context( 'CLIENT' )
*         and _root.resp_person = p_resp_person
*         and _root.tor_type   <> 'ZBB0'
*         and _root.lifecycle  <> '10'
*         and _root.tor_cat         = 'BO' -- freight booking
*         and _root.traffic_direct  = '1' -- EXPORT
*         and _root.trmodcat        = '3' -- sea
*         and _stop.log_locid       like 'KR%'
**         and localtoutc(  to_timestamp( _stop.plan_trans_time), _loc.tzone )
*         and to_dats( utctolocal(  to_timestamp( _stop.plan_trans_time), _loc.tzone ) )
*            --   etd -7 ~  etd +7
*            between to_dats(add_days( current_date, -7 ) ) and to_dats(add_days( current_date, +1 ) )
**            between to_timestamp(add_days( current_timestamp, -10 ) ) and to_timestamp(add_days( current_timestamp, +7 ) )
**         and to_timestamp(add_days( current_timestamp, -365 ) ) <= localtoutc( to_timestamp( _stop.plan_trans_time ), _loc.tzone )
**         and to_timestamp(add_days( current_timestamp, 7 ) ) >= localtoutc( to_timestamp( _stop.plan_trans_time ), _loc.tzone )
*         ;
*
*     lt_data2 =
*        SELECT
*          client,
*          resp_person,
*          tor_id,
*          days_between( etd_d, atd_d ) as delay_day,
*          CASE when atd_d = '00000000' then 'X' END as flag  --not departed
*          from :lt_data
*          WHERE row_num = 1;
*
*        RETURN SELECT
*            client,
**            resp_person,
*            count( CASE when flag = 'X'      then 1 END ) as not_departed,
*            count( CASE when delay_day = 1   then 1 END ) as delayed,
*            count( CASE when delay_day >= 2  then 1 END ) as delayed_2
*        from :lt_data2
*        GROUP BY client, resp_person; --, tor_id;


  ENDMETHOD.


  METHOD fwo_worklist_created BY DATABASE FUNCTION FOR HDB
                              LANGUAGE SQLSCRIPT
                              OPTIONS READ-ONLY USING  /scmtms/d_trqrot /scmtms/d_torite /scmtms/d_torrot.

    declare c_mandt "$ABAP.type( mandt )";
        c_mandt = session_context('CDS_CLIENT');

       lt_data =
      SELECT
      _trqrot.mandt as client,
      _trqrot.trq_id,
      _trqrot.zprj_pr,
      _torite.hbl_number,
      _fbrot.partner_mbl_id,
      _fbrot.tor_id as fb_id,
      ROW_NUMBER ( ) OVER( PARTITION by _trqrot.trq_id order by _trqrot.trq_id        desc,
                                                                _torite.hbl_number    desc,
                                                                _fbrot.partner_mbl_id desc,
                                                                _fbrot.tor_id         desc
                                                                 ) as row_num

      from "/SCMTMS/D_TRQROT" as _trqrot

*     -HBL No.
      inner join "/SCMTMS/D_TORITE" as _torite
                                    on _torite.ref_trq_root_key = _trqrot.db_key
                                   and _torite.mandt = c_mandt
*                                   and _torite.item_parent_key = ''
*                                   and _torite.main_cargo_item = 'X'

*     -MBL No.
       left outer join "/SCMTMS/D_TORITE" as _fbite
*                                          on _fbite.trq_id = _trqrot.trq_id
                                          on _fbite.ref_item_key = _torite.db_key
                                         and _fbite.mandt = c_mandt

       left outer join "/SCMTMS/D_TORROT" as _fbrot
                                          on _fbrot.db_key = _fbite.parent_key
                                         and _fbrot.tor_cat = 'BO'
                                         and _fbrot.mandt = c_mandt
*                                         and _fbrot.partner_mbl_id <> ''

       where _trqrot.created_by =  p_resp_person
       and   utctolocal(to_date( _trqrot.created_on ) ) between add_days( current_date, -100 )
                                                    and current_date
       and  _trqrot.mandt      = c_mandt
       and _trqrot.lifecycle  <> '10'
       and _trqrot.movem_type <> 'DLV'
       and _trqrot.trq_template_ind = ''
       and _torite.hbl_number  = ''


       GROUP BY _trqrot.mandt,
                _trqrot.trq_id,
                _trqrot.zprj_pr,
                _torite.hbl_number,
                _fbrot.partner_mbl_id,
                _fbrot.tor_id
       ;



* SQL NULL값 체크
* SQL은 = '' OR IS NULL 두개다 체크해주는게 안정성이 높음
* 이유는 있다가 없어진 값도 NULL로 인식하기 떄문
* 무조건 있는 값은 IS NULL로 체크
* 있을수도 있고 없을수도 있는 필드는 = '' OR IS NULL로 체크

   lt_data2 =
     select
      client,
      a.trq_id,
      a.zprj_pr,

      count( case when  trim(hbl_number) = '' or hbl_number is null  then 1 end ) as hbl_status,              -- hbl_status,   -- != <- null이 아닐경우
      count( case when  trim(partner_mbl_id) = '' or partner_mbl_id is null then 1 end ) as mbl_status,
      count( case when  trim(a.fb_id) = '' or a.fb_id is null then 1 end ) as fb_status


      from :lt_data as a
      where a.row_num = 1
      GROUP BY CLIENT, a.trq_id, zprj_pr ;


     RETURN -----------------------------------------------------------------------------------

     SELECT
      client,
      SUM( hbl_status )AS hbl_status,
      SUM( mbl_status ) AS mbl_status,
      SUM( fb_status ) AS fb_status
      FROM :lt_data2
      GROUP BY client, zprj_pr;


  ENDMETHOD.


  METHOD fwo_worklist_pne BY DATABASE FUNCTION FOR HDB
                            LANGUAGE SQLSCRIPT
                            OPTIONS READ-ONLY USING  /scmtms/d_trqrot /scmtms/d_torite /scmtms/d_torrot.


    declare c_mandt "$ABAP.type( mandt )";
    c_mandt = session_context('CDS_CLIENT');

   lt_data =
      SELECT
      _trqrot.mandt as client,
      _trqrot.trq_id,
      _trqrot.zprj_pr,
      _torite.hbl_number,
      _fbrot.partner_mbl_id,
      _fbrot.tor_id as fb_id,
      ROW_NUMBER ( ) OVER( PARTITION by _trqrot.trq_id order by _trqrot.trq_id        desc,
                                                                _torite.hbl_number    desc,
                                                                _fbrot.partner_mbl_id desc,
                                                                _fbrot.tor_id         desc
                                                                 ) as row_num
      from "/SCMTMS/D_TRQROT" as _trqrot

*     -HBL No.
      inner join "/SCMTMS/D_TORITE" as _torite
                                    on _torite.ref_trq_root_key = _trqrot.db_key
*                                   and _torite.item_parent_key = ''
*                                   and _torite.main_cargo_item = 'X'

*     -MBL No.
       left outer join "/SCMTMS/D_TORITE" as _fbite
*                                          on _fbite.trq_id = _trqrot.trq_id
                                          on _fbite.ref_item_key = _torite.db_key

       left outer join "/SCMTMS/D_TORROT" as _fbrot
                                          on _fbrot.db_key = _fbite.parent_key
                                         and _fbrot.tor_cat = 'BO'
*                                         and _fbrot.partner_mbl_id <> ''

       where _trqrot.zprj_pr   =  p_resp_person
       and   utctolocal(to_date( _trqrot.created_on ) ) between add_days( current_date, -100 )
                                                    and current_date

       and  _trqrot.mandt      = c_mandt
       and _trqrot.lifecycle  <> '10'
       and _trqrot.movem_type not in ( 'DLV', 'INF' )
       and _trqrot.trq_template_ind = ''
       and _torite.hbl_number  = ''
       GROUP BY _trqrot.mandt,
                _trqrot.trq_id,
                _trqrot.zprj_pr,
                _torite.hbl_number,
                _fbrot.partner_mbl_id,
                _fbrot.tor_id;

* SQL NULL값 체크
* SQL은 = '' OR IS NULL 두개다 체크해주는게 안정성이 높음
* 이유는 있다가 없어진 값도 NULL로 인식하기 떄문
* 무조건 있는 값은 IS NULL로 체크
* 있을수도 있고 없을수도 있는 필드는 = '' OR IS NULL로 체크

 lt_data2 =
     select
      client,
      a.trq_id,
      a.zprj_pr,

      count( case when  trim(hbl_number) = '' or hbl_number is null  then 1 end ) as hbl_status,              -- hbl_status,   -- != <- null이 아닐경우
      count( case when  trim(partner_mbl_id) = '' or partner_mbl_id is null then 1 end ) as mbl_status,
      count( case when  trim(a.fb_id) = '' or a.fb_id is null then 1 end ) as fb_status


      from :lt_data as a
      where a.row_num = 1
      GROUP BY CLIENT, a.trq_id, zprj_pr ;


     RETURN -----------------------------------------------------------------------------------

     SELECT
      client,
      SUM( hbl_status )AS hbl_status,
      SUM( mbl_status ) AS mbl_status,
      SUM( fb_status ) AS fb_status
      FROM :lt_data2
      GROUP BY client, zprj_pr;



  ENDMETHOD.


  METHOD  fwo_worklist_team  BY DATABASE FUNCTION FOR HDB
                               LANGUAGE SQLSCRIPT
                               OPTIONS READ-ONLY USING  /scmtms/d_trqrot /scmtms/d_torite /scmtms/d_torrot hrp1001 hrp1000.


    declare c_mandt "$ABAP.type( mandt )";
          c_mandt = session_context('CDS_CLIENT');



       lt_org =
          SELECT h0.mandt as client,
                 h0.short as position_objid,
            h1_wip2.objid as org_objid


          from
              (SELECT h1.objid, h1.sobid from hrp1001 h1
              where 1=1
              and h1.sclas = 'US'
              AND h1.sobid = p_resp_person             /* 현재 접속 유저 id  */
              and h1.begda <= p_date           /* 유효일 시작일이 sysdate 와 같거나 과거  */
              and h1.endda >= p_date            /* 유효일 종료일이 sysdate 와 같거나 미래  */
              and mandt = c_mandt                    /* 현재 client  */
              ) h1_wip1, hrp1001 h1_wip2, hrp1000 h0
          WHERE 1=1
          AND h1_wip2.sclas = 'S'
          AND h1_wip1.objid = h1_wip2.sobid
          and h1_wip2.begda <= p_date           /* 유효일 시작일이 sysdate 와 같거나 과거  */
          and h1_wip2.endda >= p_date            /* 유효일 종료일이 sysdate 와 같거나 미래  */
          and h1_wip2.mandt = c_mandt                /* 현재 client  */
          and h1_wip2.objid = h0.objid
          and h0.mandt = c_mandt                           /* 현재 client  */
          and h0.begda <= p_date           /* 유효일 시작일이 sysdate 와 같거나 과거  */
          and h0.endda >= p_date            /* 유효일 종료일이 sysdate 와 같거나 미래  */
          ;




      lt_data =
        select
        _trqrot.mandt as client,
        _trqrot.trq_id,
        _trqrot.zprj_pr,
        _trqrot.zpne_ext_org_id,
        _torite.hbl_number,
        _fbrot.partner_mbl_id,
        _fbrot.tor_id as fb_id,

        ROW_NUMBER ( ) OVER( PARTITION by _trqrot.trq_id order by _trqrot.trq_id        desc,
                                                                  _torite.hbl_number    desc,
                                                                  _fbrot.partner_mbl_id desc,
                                                                  _fbrot.tor_id         desc
                                                                   ) as row_num

        from "/SCMTMS/D_TRQROT" as _trqrot

*      -HBL No.
        inner join "/SCMTMS/D_TORITE" as _torite
                                      on _torite.ref_trq_root_key = _trqrot.db_key
* FWO Air에서 Item을 생성안해도 즉 TOR Root가 없어도 ref_trq_root_key = _trqrot.db_key 관계가 성립되는 데이터가 존재함
* 스탠다드 FWO Query에서는 ref_trq_root_key = _trqrot.db_key 로만 조인 관계를 맺기 때문에 앱타일과 POWL의 결과값이 미세하게 다름.
* 스탠다드 Query와 동일하게 조건을 주기 위해 main_cargo_item 조건은 삭제. (p&e pseron, creatd by 동일 적용)
*                                     and _torite.main_cargo_item = 'X'
*      -PNE ORG
        inner join :lt_org   as _pne_org
                             on _pne_org.position_objid = _trqrot.zpne_ext_org_id

*       -MBL No.
         left outer join "/SCMTMS/D_TORITE" as _fbite
*                                          on _fbite.trq_id = _trqrot.trq_id
                                            on _fbite.ref_item_key = _torite.db_key

         left outer join "/SCMTMS/D_TORROT" as _fbrot
                                            on _fbrot.db_key = _fbite.parent_key
                                           and _fbrot.tor_cat = 'BO'
*                                         and _fbrot.partner_mbl_id <> ''



*         where _trqrot.zpne_ext_org_id = '601520'--exe_pne_org
*      WHERE _TRQROT.zprj_pr   =  p_resp_person
        where  _trqrot.mandt     = c_mandt
       and   utctolocal(to_date( _trqrot.created_on ) ) between add_days( current_date, -100 )
                                                    and current_date
         and _trqrot.lifecycle  <> '10'
         and _trqrot.movem_type <> 'DLV'
         and _trqrot.trq_template_ind = ''
         and _torite.hbl_number = ''

         GROUP BY _trqrot.mandt,
                  _trqrot.trq_id,
                  _trqrot.zprj_pr,
                  _trqrot.zpne_ext_org_id,
                  _torite.hbl_number,
                  _fbrot.partner_mbl_id,
                  _fbrot.tor_id
         ;



* SQL NULL값 체크
* SQL은 = '' OR IS NULL 두개다 체크해주는게 안정성이 높음
* 이유는 있다가 없어진 값도 NULL로 인식하기 떄문
* 무조건 있는 값은 IS NULL로 체크
* 있을수도 있고 없을수도 있는 필드는 = '' OR IS NULL로 체크

     lt_data2 =
       select
        client,
        a.trq_id,
        a.zprj_pr,

        count( case when  trim(hbl_number) = '' or hbl_number is null  then 1 end ) as hbl_status,              -- hbl_status,   -- != <- null이 아닐경우
        count( case when  trim(partner_mbl_id) = '' or partner_mbl_id is null then 1 end ) as mbl_status,
        count( case when  trim(a.fb_id) = '' or a.fb_id is null then 1 end ) as fb_status


        from :lt_data as a
        where a.row_num = 1
        GROUP BY CLIENT, a.trq_id, zprj_pr ;


       RETURN -----------------------------------------------------------------------------------

       SELECT
        client,
        SUM( hbl_status )AS hbl_status,
        SUM( mbl_status ) AS mbl_status,
        SUM( fb_status ) AS fb_status
        FROM :lt_data2
        GROUP BY client, zprj_pr;


  ENDMETHOD.


  METHOD get_data BY DATABASE FUNCTION FOR HDB
                      LANGUAGE SQLSCRIPT
                      OPTIONS READ-ONLY USING /scmtms/d_torrot /scmtms/d_torstp /sapapo/loc.

    declare c_mandt "$ABAP.type( mandt )";
    c_mandt = session_context('CDS_CLIENT');

*    현글
    IF p_account = 'H' THEN

    lt_data =
    SELECT
     _torrot.mandt as client,
     COUNT(CASE when _torrot.zif_sr_st_klnet = '1' THEN 1 END )  as status1, -- s/r Not sent
     COUNT(CASE when _torrot.zif_sr_st_klnet = '2' THEN 1 END )  as status2, -- s/r sent to klnet
     COUNT(CASE when _torrot.zif_sr_st_klnet = '4' THEN 1 END )  as status3, -- Received CHECK b/l
     COUNT(CASE when _torrot.zif_sr_st_klnet in ( '2', '3', '4' ) and _torrot.zif_vgm_st_klnet = '1' THEN 1 END ) as status4, -- vgm Not sent
     COUNT(CASE when _torrot.zif_sr_st_klnet in ( '2', '3', '4' ) and _torrot.partner_mbl_id   = ''  then 1 END ) as status5  -- No mbl #

     from "/SCMTMS/D_TORROT" AS _torrot      inner join "/SCMTMS/D_TORSTP" as _torstp
                                                     on _torrot.mandt  = c_mandt -- _torstp.mandt
                                                    and _torstp.mandt  = c_mandt
                                                    and _torrot.db_key = _torstp.parent_key

                                             inner join "/SAPAPO/LOC" as _loc
                                                     on _loc.mandt     = c_mandt
                                                    and _torstp.log_loc_uuid = _loc.loc_uuid

     where
           _torrot.lifecycle     <> '10'           -- not canceled
       and _torrot.tor_cat        = 'BO'           -- fb
       and _torrot.traffic_direct = '1'            -- EXPORT
       and _torrot.tor_type      <> 'ZBB0'         -- not 'ZBB0' type
       and _torrot.trmodcat       = '3'            -- ocean
       and _torrot.resp_person    = p_user  -- Responsible Person = sy-uname

       and _torstp.stop_role      = 'MS'           -- Main stage
       and _torstp.stop_cat       = 'O'            -- Out Bound
       and _torstp.log_locid   like 'KR%'          -- pol = kr~

       and to_dats( current_timestamp ) <= to_dats( utctolocal( to_timestamp( _torstp.plan_trans_time ), _loc.tzone ) )
       and to_dats(add_days( current_timestamp, 7 ) ) >= to_dats( utctolocal( to_timestamp( _torstp.plan_trans_time ), _loc.tzone ) )

     group by _torrot.mandt;

*     협력사
     elseif p_account = 'P' then

    lt_data =

      select
     _torrot.mandt as client,
     COUNT(CASE when _torrot.zif_sr_st_klnet = '1' THEN 1 END )  as status1, -- s/r Not sent
     COUNT(CASE when _torrot.zif_sr_st_klnet = '2' THEN 1 END )  as status2, -- s/r sent to klnet
     COUNT(CASE when _torrot.zif_sr_st_klnet = '4' THEN 1 END )  as status3, -- Received CHECK b/l
     COUNT(CASE when _torrot.zif_sr_st_klnet in ( '2', '3', '4' ) and _torrot.zif_vgm_st_klnet = '1' THEN 1 END ) as status4, -- vgm Not sent
     COUNT(CASE when _torrot.zif_sr_st_klnet in ( '2', '3', '4' ) and _torrot.partner_mbl_id   = ''  then 1 END ) as status5  -- No mbl #

     from "/SCMTMS/D_TORROT" AS _torrot      inner join "/SCMTMS/D_TORSTP" as _torstp
                                                     ON _torrot.mandt  = c_mandt -- _torstp.mandt
                                                    and _torstp.mandt  = c_mandt
                                                    and _torrot.db_key = _torstp.parent_key

                                             inner join "/SAPAPO/LOC" as _loc
                                                     on _loc.mandt     = c_mandt
                                                    and _torstp.log_loc_uuid = _loc.loc_uuid

     where
           _torrot.lifecycle     <> '10'           -- not canceled
       and _torrot.tor_cat        = 'BO'           -- fb
       and _torrot.traffic_direct = '1'            -- EXPORT
       and _torrot.tor_type      <> 'ZBB0'         -- not 'ZBB0' type
       and _torrot.trmodcat       = '3'            -- ocean
       and _torrot.created_by    = p_user  -- Responsible Person = sy-uname

       and _torstp.stop_role      = 'MS'           -- Main stage
       and _torstp.stop_cat       = 'O'            -- Out Bound
       and _torstp.log_locid   like 'KR%'          -- pol = kr~

       and to_dats( current_timestamp ) <= to_dats( utctolocal( to_timestamp( _torstp.plan_trans_time ), _loc.tzone ) )
       and to_dats(add_days( current_timestamp, 7 ) ) >= to_dats( utctolocal( to_timestamp( _torstp.plan_trans_time ), _loc.tzone ) )

     group by _torrot.mandt;

     end if;

    return -----------------------------------------------------------------------------------

    SELECT *
      FROM :lt_data;



  ENDMETHOD.


  METHOD get_iss_list_user_n_team
       BY DATABASE FUNCTION FOR HDB
       LANGUAGE SQLSCRIPT
       OPTIONS READ-ONLY
       USING hrp1001 ztmfsdt0010 ztmfsdt0100 ztmfsdt0101 ztmift0320.

* 2022-09-15 d200038
*[조회조건 - 거래명세서 리스트(User)]
*1) 구분 : 전체
*2) 운영팀 : 로그인한 계정의 부서
*3) 문서상태 : 생성완료, 역분개
*4) Issue Date : 기본 셋팅일자 그대로 조회일자 (마감월기준 1일~말일)
*5) 거래명세서 생성자 : 로그인한 계정 사번

*[조회조건 - 거래명세서 리스트(Team)]
*1) 구분 : 전체
*2) 운영팀 : 로그인한 계정의 부서
*3) 문서상태 : 생성완료, 역분개
*4) Issue Date : 기본 셋팅일자 그대로 조회일자 (마감월기준 1일~말일)

* CDS View ZTMFSDD3100(User), ZTMFSDD3110(Team)을 만들고
* 하나의 Table Function으로 User인지 Team인지 Parameter에 구분을 주어 확인.

    declare lv_client nvarchar( 3 );

    lv_client = session_context( 'CLIENT' );

    -- GET first & LAST day OF CLOSE month
    lt_closedate = --
    SELECT
    to_date( CONCAT( zcloseyyyymm, '01' ) ) AS first_date,
    last_day( to_date( CONCAT( zcloseyyyymm, '01' ) ) ) AS last_date
    FROM ztmfsdt0010
    WHERE mandt = :lv_client
    AND   zclosestat = '20' --processing인 달의 1일부터 말일까지 적
    limit 1 -- up to 1 rows 의미
    ;

   lt_data =
       SELECT
       a.ziss_inv_no,
*            case when b.zinv_sts_cd != 'J' and ( c.zsend_st != '' OR C.zsend_st is not null ) then 1 else 0 end as not_confirmed,
            case when b.zinv_sts_cd != 'J' and ( c.zinv_no is not null or c.zinv_no = a.ziss_inv_no ) then 1 else 0 end as not_confirmed,
            case when c.zsend_st     = 'R' OR C.zsend_st = 'E'   THEN 1 ELSE 0 END as wait_n_error,
            CASE when C.zsend_st     = ''  OR C.zsend_st is null then 1 else 0 end as not_sent,
            row_number ( ) over( partition by a.ziss_inv_no order by a.ziss_inv_no ) as row_num
*            c.zinv_no as snd_inv_no

            from ztmfsdt0100 as a
            join :lt_closedate as d on d.first_date <= a.zinv_ymd
                                   and d.last_date  >= a.zinv_ymd

            join hrp1001 as e       on e.sobid in ( select objid from hrp1001 as e1 where e1.sclas = 'US'
                                                                                   and   e1.sobid = p_userid
                                                                                   AND   e1.begda <= current_date
                                                                                   and   e1.endda >= current_date
                                                                                   AND   e1.mandt = a.mandt )
                                   and e.objid = a.zexec_org_id
                                   and e.sclas ='S'
                                   AND E.begda <= current_date
                                   and e.endda >= current_date
                                   AND E.mandt = a.mandt

            join ztmfsdt0101 as b   on b.ziss_inv_no   = a.ziss_inv_no
                                   and b.mandt         = a.mandt
            left outer
            join ztmift0320  as c   on c.zinv_no       = a.ziss_inv_no
                                   and c.mandt         = a.mandt
                                   and c.zsend_date    = ( select MAX( zsend_date ) FROM ztmift0320
                                                           WHERE zinv_no = a.ziss_inv_no )
                                   and c.zsend_time    = ( select MAX( zsend_time ) FROM ztmift0320
                                                           WHERE zinv_no = a.ziss_inv_no )
            where a.zdel_flg in ( 'N', 'R' ) --생성완료, 역분개
*           User or Team 에 따라 동적 where 조건.
            and   a.ernam    like case  when p_division = 'U' then p_userid
                                                              else '%' end
            and   a.mandt = :lv_client;
*            and   a.zexec_org_id = '50000084' -- Test
*            group by a.ziss_inv_no,
*                     b.zinv_sts_cd, c.zsend_st,
*                     c.zinv_no;

            return select
*                case when snd_inv_no = '' or snd_inv_no is null THEN sum( not_confirmed ) end as not_confirmed,
                sum( not_confirmed ) as not_confirmed,
                sum( wait_n_error )  as wait_n_error,
                sum( not_sent )      as not_sent
                from :lt_data
                where row_num = 1;


  ENDMETHOD.


  METHOD mpa_demurrage
       BY DATABASE FUNCTION FOR HDB
       LANGUAGE SQLSCRIPT
       OPTIONS READ-ONLY
       USING ztmt3300.

    declare c_mandt "$ABAP.type( mandt )";
    c_mandt = session_context('CDS_CLIENT');


    return select
     zwarning_dem  as warning,
     zover_end_dem as over_end,
     zend_dem      as zend
     from ztmt3300
     where mandt = c_mandt;


  ENDMETHOD.


  METHOD mpa_detention
       BY DATABASE FUNCTION FOR HDB
       LANGUAGE SQLSCRIPT
       OPTIONS READ-ONLY
       USING ztmt3300.

    declare c_mandt "$ABAP.type( mandt )";
    c_mandt = session_context('CDS_CLIENT');


    return select
     zwarning_det  as warning,
     zover_end_det as over_end,
     zend_det      as zend
     from ztmt3300
     where mandt = c_mandt;


  ENDMETHOD.
ENDCLASS.
