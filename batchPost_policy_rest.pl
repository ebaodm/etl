use strict;
use threads;
use LWP;
use HTTP::Cookies;
use HTTP::Request;
use HTTP::Response;
use JSON;
use Encode;
use File::Copy;
$|=1;

my $maxproc=35;
=pod
ѭ��Ŀ¼�������ļ�����������
��Σ�1������Ŀ¼
=cut
#������
sub postbatch{
	my ($server,$ua,$token,$filename)=@_;
	$ua->timeout(99999999);
	$filename=~s/^\/datafile/\/usr\/local\/ccic_ver/;
	my $localt=localtime;
	my $jsoncontent={"path"=>$filename};
	my $jsstr=encode_json($jsoncontent);
	my $req=HTTP::Request->new(
		'POST'=>'http://'.$server.'/pa/dc/savePolicyByFile',
	);
	$req->header('authorization'=>"Bearer $token");
	$req->content_type("application/json");
	$req->content("$jsstr");
	my $res=$ua->request($req);
	print $filename."\t".$res->status_line."\t".$res->content."\n";
 	return $res->status_line;
}
#ʧ�����Է���
sub postretry{
	my($server,$ua,$token,$filename)=@_;
	my $tmp;
	until($tmp=~/^200/){
		$tmp=&postbatch($server,$ua,$token,$filename);
		sleep 1;
	}
	my $dir=$filename;
	$dir=~s/[^\/]+$//;
	$dir=~s/^\/usr\/local\/ccic_ver/\/datafile/;
	$filename=~s/^\/usr\/local\/ccic_ver/\/datafile/;
	mkdir($dir."Complete/") unless -d $dir."Complete/";
	move($filename,$dir."Complete/");
}

my @dirs=@ARGV;

my @invaliddir;
foreach(@dirs){
	print $_."Ŀ¼������\n" and next if ! -d $_;
	push @invaliddir,$_;
}

print "�޿���Ŀ¼���˳�\n" and exit 1 if @invaliddir == 0;

my ($server,$username,$password)=('10.1.15.44','ADMIN','eBao1234');
my $cookie_jar=HTTP::Cookies->new(file=>'./portal.cookies',autosave=>1);
my $ua=LWP::UserAgent->new;
my $cookie=$ua->cookie_jar($cookie_jar);
#��½ҳ��
my $res=$ua->get('http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize');
die 'get err ,url=http://'.$server.'/cas-server/login?service=http%3A%2F%2F'.$server.'%2Fcas-server%2F%2Foauth2.0%2FcallbackAuthorize' if ! $res->is_success;
my $lt=$1 if $res->content()=~/<input type=\"hidden\" name=\"lt\" value=\"([^\"]+)/;
my $execution=$1 if $res->content()=~/<input type=\"hidden\" name=\"execution\" value=\"([^\"]+)/;
#���͵�½����
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
die "��¼ʧ��" if ! defined $res->header("Location");
#��ȡ�ض���URL��ticket
my $code=$1 if $res->header("Location")=~/ticket=(.+)/;
#��ȡtoken
$res=$ua->get('http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code);
die 'get err ,url=http://'.$server.'/cas-server//oauth2.0/accessToken?client_id=key&client_secret=secret&grant_type=authorization_code&redirect_uri=http%3A%2F%2F'.$server.'%2Fportal&code='.$code if ! $res->is_success;
my $token=$1 if $res->content=~/access_token=(.+)/;
$token=~s/\&.+$//g;
die "token��ȡʧ��" if ! defined $token;
my $procs;
#ѭ��Ŀ¼
foreach my $dir (@invaliddir){
	opendir(DIR,$dir) or print "Ŀ¼��ʧ��".$!;
	#ѭ��Ŀ¼���ļ�
	while(readdir(DIR)){
		next if $_=~/^\.+$/;
		next if ! -f $dir."/".$_;
		until(threads->list < $maxproc){
			foreach(threads->list(threads::all)){
				$_->join if $_->is_joinable;
			}
			sleep 0.1;
		}
		threads->create(\&postretry,$server,$ua,$token,$dir."/".$_);
		sleep 0.3;
	}
}

until(threads->list ==0){
	foreach(threads->list(threads::all)){
		$_->join if $_->is_joinable;
	}
	sleep 0.3;
}
__END__
