#!/usr/bin/php
<?php

$data = 'POST /pub/boo HTTP/1.1
Host: 127.0.0.1
Content-Type: application/x-www-form-urlencoded
Content-Length: 96
Accept: text/json
Connection: close

{"type":"WebEvWebrtcAcquired","data":{"clientId":"gyb0BQldqtdyMZSfU0o0iQM0F2rsZIDbc","guid":17}}';

$p_fp = fsockopen('unix:///tmp/nginx-nchan', -1, $p_errno, $p_errstr);
//$p_fp = fsockopen('localhost', 8082, $p_errno, $p_errstr);
fwrite($p_fp, $data);
//this fixes it: fread($p_fp, 1); 
fclose($p_fp);
