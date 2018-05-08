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
print "参数错误" and exit 1 if @ARGV ne 1;
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

my $objid=$ARGV[0];
$req=HTTP::Request->new(GET=>'http://'.$server.':8080/dd/public/dictionary/mgmt/v1/dc/generateAllObjectFieldBinding?objectId='.$objid.'&access_token='.$token);
$res=$ua->request($req);
print encode("gbk",decode("utf8",$res->content())) and die 'get http://'.$server.':8080/dd/public/dictionary/mgmt/v1/dc/generateAllObjectFieldBinding?objectId='.$objid.'&access_token='.$token if ! $res->is_success;
my $json=new JSON;
my $jsobj=$json->decode(decode("utf8",$res->content()));
undef $res;
undef $req;
our $dbh=DBI->connect("DBI:Oracle:$tnsname",$dbuser,$dbpwd,{AutoCommit=>0});
$objid=$jsobj->{'@pk'};
exit 1 if ! defined $objid;
my $md5 = Digest::MD5->new;
$md5->add(Dumper($jsobj));
my $md5str=$md5->hexdigest;
my $sth=$dbh->prepare("select md5str from PRODUCT_MD5_OBJ where objectid='$objid'") or (print DBI->errstr and exit);
$sth->execute or (print $dbh->errstr and next);
my $dbmd5str=$sth->fetchrow_array();
$sth->finish;
undef $md5;
if(defined $dbmd5str){
	if($md5str eq $dbmd5str){
		print "对象未变化，不再重复解析\n";
		system("date");
		$dbh->disconnect;
		exit 0;
	}else{
		main($jsobj);
		$dbh->do("update PRODUCT_MD5_OBJ set md5str='$md5str',objectname='".$jsobj->{'ObjectName'}."' where objectid='".$objid."'") or (print DBI->errstr and exit);
	}
}else{
	main($jsobj);
	$dbh->do("insert into PRODUCT_MD5_OBJ(objectid,md5str,objectname) values('".$objid."','$md5str','".$jsobj->{'ObjectName'}."')") or (print DBI->errstr and exit);
}
$dbh->commit;
sub main{
	my $jsobj=$_[0];
	print $jsobj->{'@pk'}."-".$jsobj->{'@type'}."对象变更，开始解析\n";
	$dbh->do("delete from Product_field_attr_tmp where pk='".$jsobj->{'@pk'}."'");
	getfield($objid,$jsobj->{"Fields"});
}

sub getfield{
	my ($pk,$obj) = @_;
	foreach my $k (keys%{$obj}){
		my $sql2h="insert into Product_field_attr_tmp (pk , field_name,";
		my $sql2f=" values ('".$pk."','".$k."',";
		foreach my $j (keys%{$obj->{$k}}){
			my $type=reftype $obj->{$k}->{$j};
			if (! defined $type){
				$sql2h.=$j.",";
				$obj->{$k}->{$j}=~s/\'/\'\'/g;
				$sql2f.="'".$obj->{$k}->{$j}."',";
			}else{
				if(! exists($obj->{$k}->{"CodeTableId"})){
					unknowfield($k)	and next;
				}
			}
		}
		$sql2h=~s/\,$//;
		$sql2f=~s/\,$//;
		$sql2h.=")";
		$sql2f.=")";
		$dbh->do($sql2h.$sql2f) or (print DBI->errstr and exit);
	}
}