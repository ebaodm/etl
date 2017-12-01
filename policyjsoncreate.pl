use JSON;
use DBI;
use Data::Dumper;
use Time::HiRes qw/time/;
#pk,productidΪ���
my @pk=("xxx",'y');
my $productid="200019506";
my $fk;
my ($json,$relation);
my $pkcol="POLICY_NO";#���ñ�
my $fkcol="POLICY_NO";#���ñ�
my $dbh=DBI->connect('DBI:Oracle:pnccicdbdc','dc_admin','ccic726!');
$dbh->{FetchHashKeyName} = 'NAME';
print time;
print "\n";
my @lvl;
my $conf;
my $sth=$dbh->prepare("select distinct a.midtab_name,
 a.midcol_name,
 a.attributecode,
 B.MODELTYPE,
 B.NODE_LVL,
 B.RELATIONNAME,
 B.PARENTMODELTYPE
 from dm_mapping a, PRODUCT_MODELLVL b
 where a.tab_name = b.table_name
 and a.midcol_name is not null
 and nvl(B.RELATIONNAME, '1') <> 'TempPolicyCoverageList'
 and b.productid='".$productid."'");
$sth->execute();
=pod
��PRODUCT_MODELVL��ȡ��Ʒ�㼶���ֶ����ô���conf���ñ���
���
1���������͡����󡢶����������ֶι�ϵ
2�����Ӷ���֮��Ľڵ���
3�����Ӷ���֮���ϵ
4�����������
5������������ӳ�䣬����oracle��ѯ���ص�hash key�Զ���Ϊ��д��JSON���ִ�Сд��ӳ���ȥ��
=cut
while(my @resrow=$sth->fetchrow_array){
	push @lvl,$resrow[4];
	$conf->{$resrow[4]}->{$resrow[3]}->{'attributes'}->{$resrow[2]}=$resrow[1];#{�㼶}->{type}->{attr}=attrname
	$conf->{$resrow[4]}->{$resrow[3]}->{'relation'}=$resrow[5] if defined $resrow[5];#{�㼶}->{type}->{relation}=relationname
	$conf->{$resrow[4]}->{$resrow[3]}->{'parent'}=$resrow[6] if defined $resrow[6];#{�㼶]->{type}->{parent}=parent
	$conf->{$resrow[4]}->{$resrow[3]}->{'table'}=$resrow[0];#{�㼶}->{type}->{table}=tablename
	@{$conf->{$resrow[4]}->{$resrow[3]}->{'pkval'}}=@pk if $resrow[4] == 1;#{�㼶}->{type}->{pkvalue}=pkvalues
	$conf->{$resrow[4]}->{$resrow[3]}->{'pkcol'}=$pkcol;#{�㼶}->{type}->{pkcolumn}=pkcolumnname
	$conf->{$resrow[4]}->{$resrow[3]}->{'fkcol'}=$fkcol;#{�㼶}->{type}->{fkcolumn}=fkcolumnname
	$conf->{$resrow[4]}->{$resrow[3]}->{'attrmapping'}->{uc($resrow[2])}=$resrow[2];#{�㼶}->{type}->{attrmapping}->{upper(columnname)}=columnname
	$conf->{$resrow[4]}->{$resrow[3]}->{'attrmapping'}->{uc($pkcol)}=$pkcol;#{�㼶}->{type}->{attrmapping}->{upper(pkcolumnname)}=pkcolumnname
	$relation->{$resrow[3]}->{$resrow[6]}=$resrow[5] if defined $resrow[5];#{type}->{relation}=relationname
}
#��Ʒ�㼶ȥ������
my %count;
@lvl=grep { ++$count{ $_ } < 2; } @lvl;
undef %count;
@lvl=sort @lvl;
#��ѭ��Ʒ�㼶
foreach my $lv (@lvl){
	foreach my $type (keys%{$conf->{$lv}}){#��ѭtype(Policy-POLICY,...)
		my $sql="select ";
		foreach my $attr (keys%{$conf->{$lv}->{$type}->{'attributes'}}){#��ѭattr(policyid,...)
			$sql.=$conf->{$lv}->{$type}->{'attributes'}->{$attr}." as ".$attr.",";
		}
		#ƴ�ӱ����ݲ�ѯ
		$sql.=$pkcol." from ".$conf->{$lv}->{$type}->{'table'}." where ".$conf->{$lv}->{$type}->{"pkcol"}." in ('".join("','",@{$conf->{$lv}->{$type}->{'pkval'}})."')" if $lv ==1 ;
		$sql.=$pkcol." from ".$conf->{$lv}->{$type}->{'table'}." where ".$conf->{$lv}->{$type}->{"fkcol"}." in ('".join("','",@{$conf->{$lv}->{$type}->{'fkval'}})."')" if $lv !=1 ;
		my $datasth=$dbh->prepare($sql);
		$datasth->execute();
		my $data=$datasth->fetchall_hashref($conf->{$lv}->{$type}->{"pkcol"});#date->{pk}->{attr}=value
		foreach my $datapk (keys%{$data}){#����ѯ�������������ӳ������hash
			my $tmpdata;
			foreach my $attr(keys%{$data->{$datapk}}){
				#$tmpdata->{$conf->{$lv}->{$type}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr} if defined $data->{$datapk}->{$attr};
				$tmpdata->{$conf->{$lv}->{$type}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};#�������Ƿ���Ҫ
			}
			$tmpdata->{'@type'}=$type;
			push @{$json->{$type}},$tmpdata;
			push @{$conf->{$lv}->{$type}->{"pkval"}},$data->{$datapk}->{$conf->{$lv}->{$type}->{"pkcol"}} if $lv != 1;
		}
		foreach my $childtype (keys%{$conf->{$lv+1}}){#��ǰ�㼶��������Ϊ��һ�㼶�����(policy.policyid=policylob.policyid,...)
			if($conf->{$lv+1}->{$childtype}->{'parent'} eq $type and $lv+1 <= $lvl[-1]){
				@{$conf->{$lv+1}->{$childtype}->{'fkval'}}=@{$conf->{$lv}->{$type}->{"pkval"}};
			}
		}
	}
}
#���ݶ�����ϵ��װ���㼶��ϵ��JSON
foreach my $type (keys%{$relation}){
	foreach my $parenttype (keys%{$relation->{$type}}){
		for(my $parentidx=0;$parentidx < @{$json->{$parenttype}};$parentidx++){
			for(my $childidx=0;$childidx < @{$json->{$type}};$childidx++){
				if($json->{$parenttype}[$parentidx]->{$pkcol} eq $json->{$type}[$childidx]->{$fkcol} and $parenttype eq $json->{$parenttype}[$parentidx]->{'@type'}){
					push @{$json->{$parenttype}[$parentidx]->{$relation->{$type}->{$parenttype}}},$json->{$type}[$childidx];
				}
			}
		}
	}
}
print time;
print "\n";
$dbh->disconnect();
for(my $count=0;$count < @{$json->{'Policy-POLICY'}};$count++){
	print "\n#################json".($count+1)."#################\n";
	print encode_json($json->{'Policy-POLICY'}[$count])."\n";
	print Dumper($json->{'Policy-POLICY'}[$count])."\n";
}
=pod
unlink './POLICYJSON.txt' if -f './POLICYJSON.txt';
open(JSON,">>./POLICYJSON.txt");
print JSON encode_json($json->{'Policy-POLICY'});
close(JSON);
unlink './DUMP.txt' if -f './DUMP.txt';
open(DUMP,">>./DUMP.txt");
print DUMP Dumper($json->{'Policy-POLICY'});
close(DUMP);
open(CFG,'>./jsonconf.txt');
print CFG Dumper($conf);
close(CFG);
=cut

__END__