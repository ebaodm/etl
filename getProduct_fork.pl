use LWP;
use HTTP::Cookies;
use HTTP::Request;
use HTTP::Response;
use JSON;
use Encode;
use Scalar::Util qw(reftype);
use Digest::MD5;
use Data::Dumper;
use DBI;
$|=1;
system("date");
my ($server,$username,$password,$tnsname,$dbuser,$dbpwd)=('172.25.18.37','ADMIN','eBao1234','ccic_dev','dc_admin','ccic726');
my $cookie_jar=HTTP::Cookies->new(file=>'./protal.cookies',autosave=>1);
my $ua=LWP::UserAgent->new;
my $cookie=$ua->cookie_jar($cookie_jar);
my $res=$ua->get('http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize');
die 'get err ,url=http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize' if ! $res->is_success;
my $lt=$1 if $res->content()=~/<input type=\"hidden\" name=\"lt\" value=\"([^\"]+)/;
my $execution=$1 if $res->content()=~/<input type=\"hidden\" name=\"execution\" value=\"([^\"]+)/;
$res=$ua->post('http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize',
	[
		username=>$username,
		password=>$password,
		lt=>$lt,
		execution=>$execution,
		_eventId=>'submit',
		submit=>'LOGIN',
	],
	'Content-Type'=> 'application/x-www-form-urlencoded',
	'User-Agent'=>'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
);
die "登录失败" if ! defined $res->header("Location");
my $code=$1 if $res->header("Location")=~/ticket=(.+)/;
#获取token
$res=$ua->get('http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code);
die 'get err ,url=http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code if ! $res->is_success;
my $token=$1 if $res->content=~/access_token=(.+)/;
die "token获取失败" if ! defined $token;
$req=HTTP::Request->new(GET=>'http://'.$server.':8080/product/prd/v1/query/getAllProductList?access_token='.$token);
$res=$ua->request($req);
print encode("gbk",decode("utf8",$res->content())) and die 'get http://'.$server.':8080/product/prd/v1/query/getAllProductList error' if ! $res->is_success;
my $productjson=decode_json($res->content) or die "非法JSON";
print "获取所有产品\n";
foreach my $row (@{$productjson}){
	our $dbh=DBI->connect("DBI:Oracle:$tnsname",$dbuser,$dbpwd,{AutoCommit=>0}) or die "连接数据库失败：".DBI->errstr;
	our $productid=$row->{"ProductId"};
	print $productid."-".encode("gbk",$row->{"BusinessCode"})."-".encode("gbk",$row->{"ProductElementName"})."\n";
	$dbh->disconnect and next if ! defined $productid;
	$dbh->do("delete from product_product where pk='$row->{\"\@pk\"}'");
	$dbh->commit;
	$sth=$dbh->prepare("insert into product_product(
		pk,type,BusinessCode,BusinessObjectId,BusinessUUID,EffectiveFlag,EndDate,IsAutoGenerateChild,
		ProductElementCode,ProductElementId,ProductElementName,ProductId,ProductMasterId,ProductVersion,
		StartDate)values(?,?,?,?,?,?,to_date(?,'yyyy-mm-dd'),?,?,?,?,?,?,?,to_date(?,'yyyy-mm-dd'))") or (print DBI->errstr and $dbh->disconnect and next);
	$sth->bind_param(1,$row->{"\@pk"});
	$sth->bind_param(2,$row->{"\@type"});
	$sth->bind_param(3,$row->{"BusinessCode"});
	$sth->bind_param(4,$row->{"BusinessObjectId"});
	$sth->bind_param(5,$row->{"BusinessUUID"});
	$sth->bind_param(6,$row->{"EffectiveFlag"});
	$sth->bind_param(7,$row->{"EndDate"});
	$sth->bind_param(8,$row->{"IsAutoGenerateChild"});
	$sth->bind_param(9,$row->{"ProductElementCode"});
	$sth->bind_param(10,$row->{"ProductElementId"});
	$sth->bind_param(11,$row->{"ProductElementName"});
	$sth->bind_param(12,$row->{"ProductId"});
	$sth->bind_param(13,$row->{"ProductMasterId"});
	$sth->bind_param(14,$row->{"ProductVersion"});
	$sth->bind_param(15,$row->{"StartDate"});
	$sth->execute or (print DBI->errstr and $dbh->disconnect and  next);
	$dbh->commit;
	$sth->finish;
}
$dbh->disconnect;
system("perl /home/oracle/dc/ccic/script/getProductObj_fork.pl");
system("perl /home/oracle/dc/ccic/script/getObjAttr_fork.pl");
system("perl /home/oracle/dc/ccic/script/getCodeTable.pl");
system("date");