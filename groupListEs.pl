use lib '/ccicall/dc/script/Public';
use MyTools;
use LWP;
use DBI;
use JSON;
use threads;
use Encode;
use Data::Dumper;
$|=1;
$ENV{"PERL_LWP_SSL_VERIFY_HOSTNAME"}=0;
$ENV{"NLS_LANG"}='AMERICAN_AMERICA.AL32UTF8';
$ENV{"NLS_DATE_FORMAT"}='YYYY-MM-DD';
my ($tbname,$apiserver,$user,$pwd,$dbname,$dbuser,$dbpwd,$maxrows,$maxproc,$etlid)=("T_PA_PL_PERSON_INSURED","10.1.14.178","20001",'eBao1234',
	"pccictst9","dc_test","dc_test",4999,5,"groupListEs.pl");

#取token
sub gettoken{
	return {"errcode"=>1,"errmsg"=>"param error,function:gettoken"} if @_ != 3;
	my ($apiserver,$user,$pwd)=@_;
	my $ua=LWP::UserAgent->new;
	my $res=$ua->get('https://'.$apiserver.'/cas-server/login?service=http%3A%2F%2F'.$apiserver.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize');
	return{
		'errcode'=>1,
		'errmsg'=>'status line:'.$res->status_line.',url:https://'.$apiserver.'/cas-server/login?service=http%3A%2F%2F'.$apiserver.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize'
	} if ! $res->is_success;
	my $lt=$1 if $res->content()=~/<input type=\"hidden\" name=\"lt\" value=\"([^\"]+)/;
	my $execution=$1 if $res->content()=~/<input type=\"hidden\" name=\"execution\" value=\"([^\"]+)/;
	#发送登陆请求
	$res=$ua->post('https://'.$apiserver.'/cas-server/login?service=http%3A%2F%2F'.$apiserver.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize',
		[
			username=>$user,
			password=>$pwd,
			lt=>$lt,
			execution=>$execution,
			_eventId=>'submit',
			submit=>'LOGIN',
		],
		'Content-Type'=> 'application/x-www-form-urlencoded',
		'User-Agent'=>'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
	);
	return {'errcode'=>"1",'errmsg'=>"登录失败"} if ! defined $res->header("Location");
	#获取重定向URL的ticket
	my $code=$1 if $res->header("Location")=~/ticket=(.+)/;
	#获取token
	$res=$ua->get('https://'.$apiserver.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$apiserver.'%2Fportal&code='.$code);
	return{
		'errcode'=>1,
		'errmsg'=>'status line:'.$res->status_line.',url:https://'.$apiserver.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$apiserver.'%2Fportal&code='.$code
	} if ! $res->is_success;
	my $token=$1 if $res->content=~/access_token=(.+)/;
	$token=~s/\&.+$//g;
	return{
		"errcode"=>1,
		"errmsg"=>"token获取失败"
	} if ! defined $token;
	return $token;
}

#调用rest接口推送es
sub postes{
	return {'errcode'=>1,"errmsg"=>"param error,function:postes"} if @_ !=3;
	my ($apiserver,$token,$data)=@_;
	my $ua=LWP::UserAgent->new();
	$ua->timeout(600);
	my $apiurl='https://'.$apiserver.'/search/public/doSearchIndex/v1/doIndexWithMapBulk';
	my $req=HTTP::Request->new('POST'=>$apiurl);
	$req->header("Authorization"=>"Bearer ".$token);
	$req->content_type("application/json");
	$req->content(encode_json($data));

	my $res=$ua->request($req);
	return{
		"errcode"=>1,
		"errmsg"=>$res->content,
		"data"=>encode_json($data),
		"statusline"=>$res->status_line
	} if ! $res->is_success;
	my $retstr=$res->content;
	$resstr=~s/^rescontent\://;
	my $resjson=decode_json($retstr);
	return{
		"errcode"=>2,
		"errmsg"=>$res->content,
		"data"=>encode_json($data),
	} if ! $resjson->{"Status"};
	return{
		"errcode"=>0,
		"errmsg"=>$res->content,
	};
}
#属性映射实体表字段
my $props={
	"Age"=>"AGE",
	"EntityId"=>"INSURED_ID",
	"BenefitModeCode"=>"BENEFIT_MODE_CODE",
	"CustomerName"=>"CUSTOMER_NAME",
	"CustomerRoleCode"=>"CUSTOMER_ROLE_CODE",
	"EffectiveDate"=>"EFFECTIVE_DATE",
	"ExpiryDate"=>"EXPIRY_DATE",
	"IdNo"=>"ID_NO",
	"IdType"=>"ID_TYPE",
	"IndiGenderCode"=>"INDI_GENDER_CODE",
	"InsuredGroupNo"=>"INSURED_GROUP_NO",
	"InsuredId"=>"INSURED_ID",
	"OccupationCode"=>"OCCUPATION_CODE",
	"OccupationType"=>"OCCUPATION_TYPE",
	"PolicyId"=>"POLICY_ID",
	"PolicyStatus"=>"POLICY_STATUS",
	"SequenceNumber"=>"SEQUENCE_NUMBER"
};

#转时间戳字段
my $dateprop={
	"EffectiveDate"=>"",
	"ExpiryDate"=>""
};

#映射驼峰
my $propmap={
	"ENTITYID"=>"entity_id",
	"AGE"=>"Age",
	"BENEFITMODECODE"=>"BenefitModeCode",
	"CHECKINDATE"=>"CheckInDate",
	"CUSTOMERNAME"=>"CustomerName",
	"CUSTOMERROLECODE"=>"CustomerRoleCode",
	"EFFECTIVEDATE"=>"EffectiveDate",
	"EXPIRYDATE"=>"ExpiryDate",
	"FLIGHTNO"=>"FlightNo",
	"IDNO"=>"IdNo",
	"IDTYPE"=>"IdType",
	"INDIGENDERCODE"=>"IndiGenderCode",
	"INSUREDGROUPNO"=>"InsuredGroupNo",
	"INSUREDID"=>"InsuredId",
	"OCCUPATIONCODE"=>"OccupationCode",
	"OCCUPATIONTYPE"=>"OccupationType",
	"POLICYID"=>"PolicyId",
	"POLICYSTATUS"=>"PolicyStatus",
	"SEQUENCENUMBER"=>"SequenceNumber"
};

my $ua=LWP::UserAgent->new;
my $token=gettoken($apiserver,$user,$pwd);
print $token->{"errmsg"} and &writedblog($etlid,"","","E",$token->{"errmsg"},&currenttime,&currenttime,"") and exit 1 if $token->{"errcode"};
#组装动态SQL
my $sql="select ";
foreach my $attr (keys%{$props}){
#	if(exists($dateprop->{$attr})) {
#		$sql.="dc_date_ts(a.".$props->{$attr}.") as ".$attr.",";
#	}else{
		$sql.="a.".$props->{$attr}." as ".$attr.",";
#	}
}
$sql=~s/\,$//;
#$sql.=" from ".$tbname." a where policy_no='PWUA201434011110000001'";
$sql.=" from ".$tbname." a";

my $dbh=DBI->connect("DBI:Oracle:".$dbname,$dbuser,$dbpwd);
print DBI->errstr and &writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") and exit if DBI->err;
my $sth=$dbh->prepare($sql);
print DBI->errstr and &writedblog($etlid,"","",DBI->err,DBI->errstr,&currenttime,&currenttime,"") and exit if DBI->err;
$sth->execute;
my $alldata;
my $count=0;
print $sql."\n";
while(my $row=$sth->fetchrow_hashref){
	${$alldata}[$count]->{"Schema"}="InsuredPA";
	foreach my $key (keys%{$row}){
		next if ! defined $row->{$key};
		${$alldata}[$count]->{"DataEntity"}->{$propmap->{$key}}=$row->{$key};
	}
	${$alldata}[$count]->{"ParentId"}=$row->{"POLICYID"};
	if($count>=$maxrows){
		until(threads->list < $maxproc){
			foreach(threads->list(threads::joinable)){
				my $fres=$_->join;
				if($fres->{"errcode"}==1 or $fres->{"errcode"}==2){
					print "errmsg:".$fres->{"errmsg"}."\n";
					&writedblog($etlid,"","",$fres->{"errcode"},$fres->{"errmsg"},&currenttime,&currenttime,"");
					&writeeslogdata($fres->{"errmsg"},$fres->{"data"});
				}
				print "errmsg:".$fres->{"errmsg"}."\n" if $fres->{"errcode"}==0;
			}
			sleep 1;
		}
		threads->create({scalar=>1},\&postes,$apiserver,$token,$alldata);
		$count=0;
		undef $alldata;
		next;
	}
	$count++;
}
until(threads->list==0){
	foreach(threads->list(threads::joinable)){
		my $fres=$_->join;
		if($fres->{"errcode"}==1 or $fres->{"errcode"}==2){
			print "errmsg:".$fres->{"errmsg"}."\n";
			&writedblog($etlid,"","",$fres->{"errcode"},$fres->{"errmsg"},&currenttime,&currenttime,"");
			&writeeslogdata($fres->{"errmsg"},$fres->{"data"});
		}
		print "errmsg:".$fres->{"errmsg"}."\n" if $fres->{"errcode"}==0;
	}
}
my $fres=&postes($apiserver,$token,$alldata) if @{$alldata}>0;
if($fres->{"errcode"}==1 or $fres->{"errcode"}==2){
	print "errmsg:".$fres->{"errmsg"}."\n";
	&writedblog($etlid,"","",$fres->{"errcode"},$fres->{"errmsg"},&currenttime,&currenttime,"");
	&writeeslogdata($fres->{"errmsg"},$fres->{"data"});
}
print "errmsg:".$fres->{"errmsg"}."\n" if $fres->{"errcode"}==0;
__END__