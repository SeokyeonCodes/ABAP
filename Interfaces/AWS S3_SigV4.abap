  class-methods SIGV4_PUT
    importing
      !I_SERVICE type ZE_MIG_STRING default 's3'
      !I_REGION type ZE_MIG_STRING default 'ap-northeast-2'
      !I_BUCKET type ZE_MIG_STRING default 'xxxxxxxxxxxxxxx'
      !I_KEY type ZE_MIG_STRING
      !I_ACCESS_KEY type ZE_MIG_STRING
      !I_SECRET_KEY type ZE_MIG_STRING
      !I_SESSION_TOKEN type ZE_MIG_STRING
      !I_PAYLOAD type XSTRING
      !I_CONTENT_TYPE type ZE_MIG_STRING optional
    exporting
      !E_URL type STRING
      !E_HEADERS type TIHTTPNVP .

    CLASS-METHODS sha256_hex
      IMPORTING
        !i_data      TYPE xstring
      RETURNING
        VALUE(r_hex) TYPE string
      RAISING
        cx_abap_message_digest .

    CLASS-METHODS sha256_hex
      IMPORTING
        !i_data      TYPE xstring
      RETURNING
        VALUE(r_hex) TYPE string
      RAISING
        cx_abap_message_digest .

    CLASS-METHODS utf8_to_xstring
      IMPORTING
        !i_text    TYPE string
      RETURNING
        VALUE(r_x) TYPE xstring .        

    CLASS-METHODS hmac_sha256_bin
      IMPORTING
        !i_key       TYPE xstring
        !i_data      TYPE string
      RETURNING
        VALUE(r_bin) TYPE xstring
      RAISING
        cx_abap_message_digest .

    CLASS-METHODS hmac_sha256
      IMPORTING
        !i_key        TYPE xstring OPTIONAL
        !i_data       TYPE string OPTIONAL
      RETURNING
        VALUE(rv_hex) TYPE string .

    CLASS-METHODS return_http_header_set
      IMPORTING
        !i_host              TYPE string OPTIONAL
        !i_content_sha256    TYPE string OPTIONAL
        !i_amzdate           TYPE string OPTIONAL
        !i_token             TYPE string
        !i_authorization     TYPE string OPTIONAL
        !i_content_type      TYPE string OPTIONAL
        !i_content_len       TYPE i OPTIONAL
      RETURNING
        VALUE(rt_header_set) TYPE tihttpnvp .
        
  METHOD sigv4_put.
* Ref to:
*    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
*    https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
*    https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
*    https://www.strongpasswordgenerator.org/ko/sha256-hash-generator/  (Hash 변환)
*    https://www.devglan.com/online-tools/hmac-sha256-online            (HMAC-SHA256 변환)

    DATA: lv_host           TYPE string,
          lv_content_length TYPE i,
*          lv_content_type   TYPE string VALUE 'application/octet-stream'.
          lv_content_type   TYPE string VALUE 'application/gzip'.
*          lv_content_type TYPE string VALUE 'text/csv'.
*          lv_content_type TYPE string VALUE 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'.
    CLEAR:
      e_url,
      e_headers.

    IF i_content_type IS NOT INITIAL.
      lv_content_type = i_content_type.
    ENDIF.

    DATA(lv_http_method) = |PUT|.

    " S3 Endpoint (버킷 + 리전)
    lv_host = |{ i_bucket }.s3.{ i_region }.amazonaws.com|.
    " 최종 요청 URL (PUT 대상 오브젝트)
    e_url   = |https://{ lv_host }/{ i_key }|.

    " ───────────────────────────────
    " Step 1: 타임스탬프 및 페이로드 해시
    " ───────────────────────────────
    DATA(lv_timestamp)    = iso8601_utc( ).
    DATA(lv_date)         = datestamp_utc( ).
    TRY.
        DATA(lv_payload_hash) = sha256_hex( i_payload ).
      CATCH cx_abap_message_digest.
    ENDTRY.

    " ───────────────────────────────
    " Step 2: Canonical Headers 준비
    " ───────────────────────────────
*    DATA(lv_hdr_host)             = lv_host.
*    DATA(lv_hdr_amzdate)          = lv_timestamp.
*    DATA(lv_hdr_amzsecuritytoken) = i_session_token.
*    DATA(lv_hdr_amzcontentsha256) = lv_payload_hash.
*    " content-type, content-length은 필요 시 추가 가능
**    DATA(lv_hdr_contenttype)       = lv_content_type.
**    DATA(lv_hdr_content_length)    = |{ xstrlen( i_payload ) }|.

    " Canonical URI (오브젝트 키)
    DATA(lv_canonicaluri)      = |/{ i_key }|.
    " Canonical Headers 문자열 (정렬된 헤더)
    DATA(lv_canonical_headers) =
      |host:{ lv_host }\n| &&
*      |accept-encoding:gzip\n| && " 💡 Content-Encoding: gzip 추가
*      |content-type:{ lv_content_type }\n| &&
      |x-amz-content-sha256:{ lv_payload_hash }\n| &&
      |x-amz-date:{ lv_timestamp }\n|.

    IF i_session_token IS NOT INITIAL.
      lv_canonical_headers = |{ lv_canonical_headers }x-amz-security-token:{ i_session_token }\n|.
    ENDIF.


    " Signed Headers 목록 (CanonicalHeaders에 포함된 헤더 이름)
    DATA(lv_signed_headers) =
*      `host;accept-encoding;x-amz-content-sha256;x-amz-date`.
*      `host;accept-encoding;content-type;x-amz-content-sha256;x-amz-date`.
      `host;x-amz-content-sha256;x-amz-date`.
*      `host;content-type;x-amz-content-sha256;x-amz-date`.
    IF i_session_token IS NOT INITIAL.
      lv_signed_headers = |{ lv_signed_headers };x-amz-security-token|.
    ENDIF.


    " ───────────────────────────────
    " Step 3: Canonical Request 생성
    " ───────────────────────────────
    " Canonical Request 포맷:
    " <HTTPMethod>\n
    " <CanonicalURI>\n
    " <CanonicalQueryString>\n
    " <CanonicalHeaders>\n
    " <SignedHeaders>\n
    " <HashedPayload>
    DATA(lv_canonical_request) =
      |{ lv_http_method }\n|       &&
      |{ lv_canonicaluri }\n|      &&
      |\n|                         &&       " No query string for simple PUT
      |{ lv_canonical_headers }\n| &&
      |{ lv_signed_headers }\n|    &&
      |{ lv_payload_hash }|.


    " ───────────────────────────────
    " Step 4: String to Sign 생성
    " ───────────────────────────────
    " Scope = <YYYYMMDD>/<region>/<service>/aws4_request
    DATA(lv_scope) = |{ lv_date }/{ i_region }/{ i_service }/aws4_request|.

    " Canonical Request 해시
    TRY.
        DATA(lv_hashed_canonical) = sha256_hex( utf8_to_xstring( lv_canonical_request ) ).
      CATCH cx_abap_message_digest.
    ENDTRY.

    " StringToSign 포맷:
    " Algorithm \n
    " RequestDateTime \n
    " CredentialScope  \n
    " HashedCanonicalReques
    " Ex)
    "    "AWS4-HMAC-SHA256" + "\n" +
    "    timeStampISO8601Format + "\n" +
    "    <Scope> + "\n" +
    "  Hex(SHA256Hash(<CanonicalRequest>))
    DATA(lv_string_to_sign) =
      |AWS4-HMAC-SHA256\n| &&
      |{ lv_timestamp }\n| &&
      |{ lv_scope }\n| &&
      |{ lv_hashed_canonical }|.

    " ───────────────────────────────
    " Step 5: Signing Key 생성
    " ───────────────────────────────
    " DateKey              = HMAC-SHA256("AWS4"+"<SecretAccessKey>", "<YYYYMMDD>")
    " DateRegionKey        = HMAC-SHA256(<DateKey>, "<aws-region>")
    " DateRegionServiceKey = HMAC-SHA256(<DateRegionKey>, "<aws-service>")
    " SigningKey           = HMAC-SHA256(<DateRegionServiceKey>, "aws4_request")
    DATA(secretaccesskey)      = utf8_to_xstring( |AWS4{ i_secret_key }| ).
    TRY.
        DATA(datekey)              = hmac_sha256_bin( i_key = secretaccesskey      i_data = lv_date ).
        DATA(dateregionkey)        = hmac_sha256_bin( i_key = datekey              i_data = i_region ).
        DATA(dateregionservicekey) = hmac_sha256_bin( i_key = dateregionkey        i_data = i_service ).
        DATA(signingkey)           = hmac_sha256_bin( i_key = dateregionservicekey i_data = 'aws4_request' ).
      CATCH cx_abap_message_digest.
    ENDTRY.
    " 최종 Signature
    DATA(lv_signature) = to_lower_hex( hmac_sha256( i_key = signingkey i_data = lv_string_to_sign ) ).

    " ───────────────────────────────
    " Step 6: Authorization 헤더 생성
    " ───────────────────────────────
    DATA(lv_auth) =
      |AWS4-HMAC-SHA256 Credential={ i_access_key }/{ lv_scope }, SignedHeaders={ lv_signed_headers }, Signature={ lv_signature }|.

    " ───────────────────────────────
    " Step 7: 최종 헤더 세트 반환
    " ───────────────────────────────
*    DATA(lv_content_length) = xstrlen( i_payload ).
    e_headers = return_http_header_set(
                  i_host           = lv_host
                  i_content_sha256 = lv_payload_hash
                  i_amzdate        = lv_timestamp
                  i_token          = i_session_token
                  i_authorization  = lv_auth
                  i_content_type   = lv_content_type
                  i_content_len    = lv_content_length ).

  ENDMETHOD.

  METHOD iso8601_utc.
    DATA:
      lv_ts TYPE string,
      lv_dt TYPE string,
      lv_tm TYPE string.
    GET TIME STAMP FIELD DATA(ts).
    lv_ts = ts.
    lv_dt = lv_ts(8).
    lv_tm = lv_ts+8(6).
    r_ts = lv_dt && 'T' && lv_tm && 'Z'.
  ENDMETHOD.

  METHOD datestamp_utc.
    DATA:
      lv_ts TYPE string.

    GET TIME STAMP FIELD DATA(ts).
    lv_ts = ts.
    r_ds = lv_ts(8).
  ENDMETHOD.
  
  METHOD sha256_hex.
*Signature Calculations for the Authorization Header: Transferring Payload in a Single Chunk (AWS Signature Version 4)
* https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

    CLEAR r_hex.

    " SHA-256 해시 계산 (raw 데이터 기준)
    TRY.
* Same as Hex(SHA256Hash(<payload>) in AWS document
        CALL METHOD cl_abap_message_digest=>calculate_hash_for_raw
          EXPORTING
            if_algorithm  = 'SHA256'           " Hash Algorithm
            if_data       = i_data
          IMPORTING
            ef_hashstring = r_hex.

        r_hex = to_lower( r_hex ).
    ENDTRY.

  ENDMETHOD.

  METHOD sha256_hex.
*Signature Calculations for the Authorization Header: Transferring Payload in a Single Chunk (AWS Signature Version 4)
* https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

    CLEAR r_hex.

    " SHA-256 해시 계산 (raw 데이터 기준)
    TRY.
* Same as Hex(SHA256Hash(<payload>) in AWS document
        CALL METHOD cl_abap_message_digest=>calculate_hash_for_raw
          EXPORTING
            if_algorithm  = 'SHA256'           " Hash Algorithm
            if_data       = i_data
          IMPORTING
            ef_hashstring = r_hex.

        r_hex = to_lower( r_hex ).
    ENDTRY.

  ENDMETHOD.      

  METHOD utf8_to_xstring.

    CLEAR r_x.

*    DATA(encoding) = cl_abap_codepage=>convert_to( source = i_text codepage = 'UTF-8' ). " UTF-8
*    r_x = encoding.
    TRY.
        r_x = cl_abap_hmac=>string_to_xstring( if_input = i_text  ).
      CATCH cx_abap_message_digest.
    ENDTRY.
*    CALL FUNCTION 'SCMS_STRING_TO_XSTRING'
*      EXPORTING
*        text   = i_text
**        encoding
*      IMPORTING
*        buffer = r_x.

  ENDMETHOD.  

  METHOD hmac_sha256_bin.

    CLEAR r_bin.

    cl_abap_hmac=>calculate_hmac_for_char(
      EXPORTING
        if_algorithm     = 'SHA256'           " Hash Algorithm
        if_key           = i_key
        if_data          = i_data
        if_length        = strlen( i_data )
      IMPORTING
        ef_hmacxstring   = r_bin
    ).

  ENDMETHOD.  

  METHOD hmac_sha256.

    CLEAR rv_hex.
*
    DATA(lo_hmac) = cl_abap_hmac=>get_instance( if_algorithm = 'SHA256' if_key = i_key ).
    lo_hmac->init( i_key ).
    lo_hmac->update( lo_hmac->string_to_xstring( i_data ) ).

    lo_hmac->final( ).

    rv_hex = lo_hmac->to_string( ).

*    cl_abap_hmac=>calculate_hmac_for_char(
*      EXPORTING
*        if_algorithm     = 'SHA256'           " Hash Algorithm
*        if_key           = i_key
*        if_data          = i_data
*        if_length        = strlen( i_data )
*      IMPORTING
*        ef_hmacstring    = rv_hex
**        ef_hmacxstring   = r_bin
**        ef_hmacb64string = DATA(ef_hmacb64string)
*    ).

  ENDMETHOD.  

  METHOD return_http_header_set.

    CLEAR rt_header_set.

    IF i_host IS NOT INITIAL.
      APPEND VALUE #( name = 'host' value = i_host ) TO rt_header_set.
    ENDIF.

    " 설정하지 않으면 S3에서 파일 다운로드 시 HTML 로 다운로드 됨.
    IF i_content_type IS NOT INITIAL.
      APPEND VALUE #( name = 'Content-Type' value = i_content_type ) TO rt_header_set.
      IF i_content_type = 'application/gzip'.
        APPEND VALUE #( name = 'Content-Encoding' value = 'gzip' ) TO rt_header_set. " 업로드 파일이 Gzip 일 때 Set 필수
      ENDIF.
    ENDIF.
*
*    IF i_content_len IS NOT INITIAL.
*      APPEND VALUE #( name = 'Content-Length' value = i_content_len ) TO rt_header_set.
*    ENDIF.

    IF i_content_sha256 IS NOT INITIAL.
      APPEND VALUE #( name = 'x-amz-content-sha256' value = i_content_sha256 ) TO rt_header_set.
    ENDIF.

    IF i_amzdate IS NOT INITIAL.
      APPEND VALUE #( name = 'x-amz-date' value = i_amzdate ) TO rt_header_set.
    ENDIF.

    IF i_token IS NOT INITIAL.
      APPEND VALUE #( name = 'x-amz-security-token' value = i_token ) TO rt_header_set.
    ENDIF.

    IF i_authorization IS NOT INITIAL.
      APPEND VALUE #( name = 'Authorization' value = i_authorization ) TO rt_header_set.
    ENDIF.


  ENDMETHOD.    


* After Sigv$
    CLASS-METHODS exec_http_put_method
      IMPORTING
        iv_payload    TYPE xstring
        iv_url        TYPE string
        it_header     TYPE tihttpnvp
      RETURNING
        VALUE(rv_msg) TYPE string.
        
  METHOD exec_http_put_method.

    CLEAR rv_msg.

    CALL METHOD cl_http_client=>create_by_url
      EXPORTING
        url                        = iv_url
      IMPORTING
        client                     = DATA(lo_client)
      EXCEPTIONS
        argument_not_found         = 1                " Communication parameter (host or service) not available
        plugin_not_active          = 2                " HTTP/HTTPS communication not available
        internal_error             = 3                " Internal error (e.g. name too long)
        oa2c_set_token_error       = 4
        oa2c_missing_authorization = 5
        oa2c_invalid_config        = 6
        oa2c_invalid_parameters    = 7
        oa2c_invalid_scope         = 8
        oa2c_invalid_grant         = 9
        OTHERS                     = 10.
    IF sy-subrc <> 0.
      rv_msg = SWITCH #( sy-subrc
        WHEN 1  THEN 'Communication parameter (host or service) not available'
        WHEN 3  THEN 'HTTP/HTTPS communication not available'
        WHEN 4  THEN 'Internal error'
        WHEN 5  THEN 'set_token_error      '
        WHEN 6  THEN 'missing_authorization'
        WHEN 7  THEN 'invalid_config       '
        WHEN 8  THEN 'invalid_parameters   '
        WHEN 9  THEN 'invalid_scope        '
        WHEN 10 THEN 'invalid_grant        ' ).
      RETURN.
    ENDIF.

    lo_client->request->set_method( 'PUT' ).
    lo_client->request->set_data( iv_payload ).

    LOOP AT it_header ASSIGNING FIELD-SYMBOL(<ls_http_headers>).
      lo_client->request->set_header_field( name = <ls_http_headers>-name value = <ls_http_headers>-value ).
    ENDLOOP.


    lo_client->send(
      EXCEPTIONS
        http_communication_failure = 1                  " Communication Error
        http_invalid_state         = 2                  " Invalid state
        http_processing_failed     = 3                  " Error when processing method
        http_invalid_timeout       = 4                  " Invalid Time Entry
        OTHERS                     = 5 ).
    IF sy-subrc <> 0.
      rv_msg = SWITCH #( sy-subrc WHEN 1 THEN 'Communication Error'
                                  WHEN 2 THEN 'Invalid state'
                                  WHEN 3 THEN 'Error when processing method'
                                  WHEN 4 THEN 'Invalid Time Entry'
                                  WHEN 5 THEN 'HTTP Send Others Error' ).
      lo_client->close( ).
      RETURN.
    ENDIF.

    lo_client->receive(
      EXCEPTIONS
        http_communication_failure = 1                " Communication Error
        http_invalid_state         = 2                " Invalid state
        http_processing_failed     = 3                " Error when processing method
        OTHERS                     = 4 ).
    IF sy-subrc <> 0.
      rv_msg = SWITCH #( sy-subrc WHEN 1 THEN 'Communication Error'
                                  WHEN 2 THEN 'Invalid state'
                                  WHEN 3 THEN 'Error when processing method'
                                  WHEN 4 THEN 'HTTP Send Others Error' ).
    ENDIF.

    lo_client->response->get_status(
      IMPORTING
        code   = DATA(lv_status)
        reason = DATA(lv_reason) ).

    IF lv_status = '200'. " 200 OK"
      rv_msg = '200'.
    ELSE.
      DATA(lv_response) = lo_client->response->get_cdata( ).
      rv_msg = |{ rv_msg }: { lv_response }|.
    ENDIF.

    lo_client->close( ).

  ENDMETHOD.  
