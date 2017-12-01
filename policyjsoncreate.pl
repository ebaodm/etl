use JSON;
use DBI;
use Data::Dumper;
use Time::HiRes qw/time/;
#pk,productid为入参
my @pk=("xxx",'y');
my $productid="200019506";
my $fk;
my ($json,$relation);
my $pkcol="POLICY_NO";#配置表
my $fkcol="POLICY_NO";#配置表
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
从PRODUCT_MODELVL获取产品层级及字段配置存入conf配置变量
存放
1、对象类型、对象、对象属性与字段关系
2、主从对象之间的节点名
3、主从对象之间关系
4、对象主外键
5、对象属性名映射，由于oracle查询返回的hash key自动变为大写，JSON区分大小写，映射回去）
=cut
while(my @resrow=$sth->fetchrow_array){
	push @lvl,$resrow[4];
	$conf->{$resrow[4]}->{$resrow[3]}->{'attributes'}->{$resrow[2]}=$resrow[1];#{层级}->{type}->{attr}=attrname
	$conf->{$resrow[4]}->{$resrow[3]}->{'relation'}=$resrow[5] if defined $resrow[5];#{层级}->{type}->{relation}=relationname
	$conf->{$resrow[4]}->{$resrow[3]}->{'parent'}=$resrow[6] if defined $resrow[6];#{层级]->{type}->{parent}=parent
	$conf->{$resrow[4]}->{$resrow[3]}->{'table'}=$resrow[0];#{层级}->{type}->{table}=tablename
	@{$conf->{$resrow[4]}->{$resrow[3]}->{'pkval'}}=@pk if $resrow[4] == 1;#{层级}->{type}->{pkvalue}=pkvalues
	$conf->{$resrow[4]}->{$resrow[3]}->{'pkcol'}=$pkcol;#{层级}->{type}->{pkcolumn}=pkcolumnname
	$conf->{$resrow[4]}->{$resrow[3]}->{'fkcol'}=$fkcol;#{层级}->{type}->{fkcolumn}=fkcolumnname
	$conf->{$resrow[4]}->{$resrow[3]}->{'attrmapping'}->{uc($resrow[2])}=$resrow[2];#{层级}->{type}->{attrmapping}->{upper(columnname)}=columnname
	$conf->{$resrow[4]}->{$resrow[3]}->{'attrmapping'}->{uc($pkcol)}=$pkcol;#{层级}->{type}->{attrmapping}->{upper(pkcolumnname)}=pkcolumnname
	$relation->{$resrow[3]}->{$resrow[6]}=$resrow[5] if defined $resrow[5];#{type}->{relation}=relationname
}
#产品层级去重排序
my %count;
@lvl=grep { ++$count{ $_ } < 2; } @lvl;
undef %count;
@lvl=sort @lvl;
#轮循产品层级
foreach my $lv (@lvl){
	foreach my $type (keys%{$conf->{$lv}}){#轮循type(Policy-POLICY,...)
		my $sql="select ";
		foreach my $attr (keys%{$conf->{$lv}->{$type}->{'attributes'}}){#轮循attr(policyid,...)
			$sql.=$conf->{$lv}->{$type}->{'attributes'}->{$attr}." as ".$attr.",";
		}
		#拼接表数据查询
		$sql.=$pkcol." from ".$conf->{$lv}->{$type}->{'table'}." where ".$conf->{$lv}->{$type}->{"pkcol"}." in ('".join("','",@{$conf->{$lv}->{$type}->{'pkval'}})."')" if $lv ==1 ;
		$sql.=$pkcol." from ".$conf->{$lv}->{$type}->{'table'}." where ".$conf->{$lv}->{$type}->{"fkcol"}." in ('".join("','",@{$conf->{$lv}->{$type}->{'fkval'}})."')" if $lv !=1 ;
		my $datasth=$dbh->prepare($sql);
		$datasth->execute();
		my $data=$datasth->fetchall_hashref($conf->{$lv}->{$type}->{"pkcol"});#date->{pk}->{attr}=value
		foreach my $datapk (keys%{$data}){#将查询结果集作属性名映射后放入hash
			my $tmpdata;
			foreach my $attr(keys%{$data->{$datapk}}){
				#$tmpdata->{$conf->{$lv}->{$type}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr} if defined $data->{$datapk}->{$attr};
				$tmpdata->{$conf->{$lv}->{$type}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};#空属性是否需要
			}
			$tmpdata->{'@type'}=$type;
			push @{$json->{$type}},$tmpdata;
			push @{$conf->{$lv}->{$type}->{"pkval"}},$data->{$datapk}->{$conf->{$lv}->{$type}->{"pkcol"}} if $lv != 1;
		}
		foreach my $childtype (keys%{$conf->{$lv+1}}){#当前层级的主键作为下一层级的外键(policy.policyid=policylob.policyid,...)
			if($conf->{$lv+1}->{$childtype}->{'parent'} eq $type and $lv+1 <= $lvl[-1]){
				@{$conf->{$lv+1}->{$childtype}->{'fkval'}}=@{$conf->{$lv}->{$type}->{"pkval"}};
			}
		}
	}
}
#根据对象间关系组装出层级关系的JSON
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