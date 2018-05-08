use DBI;
use JSON;
use LWP;
use Encode;
use HTTP::Cookies;
use HTTP::Request;
use HTTP::Response;

our $dbh=DBI->connect("DBI:Oracle:ccic_dev","dc_admin","ccic726",{AutoCommit=>0});
my ($server,$username,$password)=('172.25.18.37','ADMIN','eBao1234');
my $cookie_jar=HTTP::Cookies->new(file=>'./protal.cookies',autosave=>1);
my $ua=LWP::UserAgent->new;
my $cookie=$ua->cookie_jar($cookie_jar);
#µÇÂ¼
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
die "µÇÂ¼Ê§°Ü" if ! defined $res->header("Location");
my $code=$1 if $res->header("Location")=~/ticket=(.+)/;
#»ñÈ¡token
$res=$ua->get('http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code);
die 'get err ,url=http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code if ! $res->is_success;
my $token=$1 if $res->content=~/access_token=(.+)/;
die "token»ñÈ¡Ê§°Ü" if ! defined $token;
my $sth=$dbh->prepare("select distinct a.codetableid from product_field_attr_tmp a where a.codetableid is not null");
die DBI->errstr if DBI->err;
$sth->execute();
die DBI->errstr if DBI->err;
while(my $codetbid=$sth->fetchrow_array){
	$req=HTTP::Request->new(GET=>'http://'.$server.':8080/dd/public/codetable/v1/data/list/'.$codetbid.'?access_token='.$token);
	$res=$ua->request($req);
	print 'get err ,url=http://'.$server.':8080/dd/public/codetable/v1/data/list/'.$codetbid.'?access_token='.$token."\n" and print encode("gbk",decode("utf8",$res->content))."\n" and next if ! $res->is_success;
	my $json=new JSON;
	my $jsobj=$json->decode(decode("utf8",$res->content()));
	undef $res;
	undef $req;
	$dbh->do("delete from Product_Attr_Code_tmp where codetableid='$codetbid'");
	print DBI->errstr and next if DBI->err;
	foreach my $codeline(@{$jsobj}){
	my ($hsql,$fsql)=("insert into Product_Attr_Code_tmp(CodeTableId,","values($codetbid,");
		foreach my $codeattr(keys%{$codeline}){
			next if $codeattr eq "ConditionFields";
			$hsql.=$codeattr.",";
			$codeline->{$codeattr}=~s/\'/\'\'/g;
			$fsql.="'".$codeline->{$codeattr}."',";
		}
		$hsql=~s/\,$//;
		$fsql=~s/\,$//;
		$hsql.=")";
		$fsql.=")";
		$dbh->do($hsql.$fsql);
		print DBI->errstr and next if DBI->err;
	}
	$dbh->commit;
}
__END__
