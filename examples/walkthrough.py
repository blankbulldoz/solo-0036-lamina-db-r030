#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics','write-admission']
class Session:
 def __init__(self,binary,root,restore=None,args=()):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.extra=list(args);self.process=None;self.log=None
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}',*self.extra]
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def commit(self,tx,status=200):return self.call('POST',f'/transactions/{tx}/commit',status=status)
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 limits=('-max-instance-bytes','4600') if name=='write-admission' else ()
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source',args=limits).start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   elif name=='write-admission':
    # The instance budget is 4600 bytes and the seeded relation already
    # retains ~2700, so the ledger, not an arbitrary key count, governs
    # admission.
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    s.call('POST','/relations',{'name':'aux','primary':'id','columns':[{'name':'id','type':'int'},{'name':'payload','type':'text','nullable':True}]},201)
    start=s.stats()['memory'];require(start['instance_bytes']>start['version_bytes'] and start['index_bytes']>0,'index candidates not charged')
    reader=s.begin(True);writer=s.begin()
    big=lambda n:{'sample':n,'band':'x'*400,'amplitude':n,'valid':True}
    s.put(writer,big(1));s.put(writer,big(2))
    def rejection(call):
     try:call()
     except RuntimeError as error:
      require('got 429' in str(error),f'unexpected failure {error}')
      return json.loads(str(error).split('got 429: ',1)[1])['error']
     raise RuntimeError('capacity limit not enforced')
    refused=rejection(lambda:s.put(writer,big(3)))
    require(refused['details']['scope']=='instance_bytes' and refused['details']['transaction_preserved'],'instance refusal lost the session')
    active=next(t for t in s.stats()['active'] if t['id']==writer)
    require(active['staged_keys']==2 and active['staged_bytes']>0,'accepted edits vanished after refusal')
    require(s.query(transaction=writer,where=[{'column':'sample','op':'=','value':1}])['rows'][0]['band'].startswith('xxx'),'read own write after refusal failed')
    require(s.query(transaction=writer)['matched']==7,'staged rows not visible to owner')
    # Replacing an already staged key is charged as a difference, so the
    # transaction can shrink without rolling back.
    shrunk=s.put(writer,{'sample':1,'band':'s','amplitude':1,'valid':True})
    require(shrunk['transaction']['staged_keys']==2 and shrunk['memory']['private_bytes']<active['staged_bytes'],'same-key replacement did not release bytes')
    require(s.put(writer,{'sample':2,'band':'t','amplitude':2,'valid':True})['memory']['private_bytes']<shrunk['memory']['private_bytes'],'second replacement did not release bytes')
    # A first-write tombstone is charged like any other new key; a tombstone
    # replacing an already staged key is a smaller final write and releases the
    # row charge. Both keep the session and earlier edits intact.
    before_tomb=s.stats()['memory']['private_bytes']
    absent=s.call('POST',f'/transactions/{writer}/delete',{'relation':'spectra','key':999})
    require(absent['memory']['private_bytes']>before_tomb and absent['transaction']['staged_keys']==3,'new tombstone was not charged')
    before_tomb=absent['memory']['private_bytes']
    tomb=s.call('POST',f'/transactions/{writer}/delete',{'relation':'spectra','key':2})
    require(tomb['memory']['private_bytes']<before_tomb and tomb['transaction']['staged_keys']==3,'staged-key tombstone did not release the row charge')
    # A different relation shares one ledger, so released bytes are reusable
    # there. Once the ceiling is reached again, the cross-relation put is
    # refused by the same instance scope, not a second reservation.
    across=s.call('POST',f'/transactions/{writer}/put',{'relation':'aux','row':{'id':1,'payload':'y'*100}})
    require(across['memory']['private_bytes']>tomb['memory']['private_bytes'],'released bytes not reusable across relations')
    cross_refused=rejection(lambda:s.call('POST',f'/transactions/{writer}/put',{'relation':'aux','row':{'id':2,'payload':'y'*2000}}))
    require(cross_refused['details']['scope']=='instance_bytes','cross-relation write not admitted by same ledger')
    freed=s.call('POST',f'/transactions/{writer}/delete',{'relation':'spectra','key':3})
    committed=s.commit(writer)
    after_commit=s.stats()['memory']
    require(committed['writes']==5 and after_commit['private_bytes']==0,'commit did not release private charge')
    require(s.query(transaction=None,where=[{'column':'sample','op':'=','value':2}])['matched']==0,'staged-key tombstone not published')
    require(s.query(where=[{'column':'sample','op':'=','value':1}])['rows'][0]['band']=='s','accepted edits not committed atomically')
    # A long reader pins the pre-commit history. Vacuum succeeds as a request
    # but reports zero reclaimed bytes, so the ledger must not pretend space
    # came back.
    head=s.stats()['sequence'];pinned=s.call('POST','/vacuum',{'horizon':head})
    require(pinned['pinned'] and pinned['reclaimed_bytes']==0 and pinned['index_reclaimed_bytes']==0 and pinned['instance_bytes']==after_commit['instance_bytes'],'pinned vacuum claimed reclamation')
    require(s.query(transaction=reader,where=[{'column':'sample','op':'=','value':1}])['rows'][0]['band']=='b1','long read lost its snapshot')
    s.call('POST',f'/transactions/{reader}/rollback')
    reclaimed=s.call('POST','/vacuum',{'horizon':head})
    require(not reclaimed['pinned'] and reclaimed['reclaimed_bytes']>0,'closed reader did not release history')
    reduced=s.stats()['memory']
    require(reduced['instance_bytes']==after_commit['instance_bytes']-reclaimed['reclaimed_bytes']-reclaimed['index_reclaimed_bytes'],'ledger did not follow real removal')
    s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    # Space really was returned: another writer's write set is admitted.
    again=s.begin()
    s.put(again,{'sample':20,'band':'z'*400,'amplitude':20,'valid':True})
    s.commit(again)
    s.call('POST','/checkpoint');s.stop()
    # Reopen with a tighter per-transaction budget to separate the two refusal
    # scopes: the ledger restarts from durable state, and transaction bytes now
    # bind before instance bytes.
    s=Session(binary,s.root,args=('-max-instance-bytes','8000','-max-transaction-bytes','1800')).start()
    restarted=s.stats()['memory']
    require(restarted['private_bytes']==0 and restarted['version_bytes']>0,'restart ledger not rebuilt')
    require(restarted['instance_bytes']==restarted['version_bytes']+restarted['index_bytes'],'restart ledger components disagree')
    tight_tx=s.begin()
    s.put(tight_tx,big(30));s.put(tight_tx,big(31))
    tight=rejection(lambda:s.put(tight_tx,big(32)))
    require(tight['details']['scope']=='transaction_bytes' and tight['details']['transaction_preserved'],'transaction scope not reported')
    s.put(tight_tx,{'sample':30,'band':'q','amplitude':30,'valid':True})
    s.put(tight_tx,big(32));tight_commit=s.commit(tight_tx)
    require(s.query(where=[{'column':'sample','op':'=','value':32}])['matched']==1,'retried write set did not commit')
    # A commit can also be refused by the instance ledger even though every
    # individual put was admitted: the durable version row and the new index
    # candidate only exist at commit time. Measure the rebuilt ledger, restart
    # with one byte of headroom over the staged row, and show that the refusal
    # preserves the session and its edits so they can be shrunk and retried.
    base=s.stats()['memory']['instance_bytes']
    s.call('POST','/checkpoint');s.stop()
    s=Session(binary,s.root,args=('-max-instance-bytes',str(base+1302))).start()
    require(s.stats()['memory']['instance_bytes']==base,'restart ledger differs from durable ledger')
    retry_tx=s.begin();long_row={'sample':50,'band':'w'*1000,'amplitude':9999,'valid':True}
    staged_commit=s.put(retry_tx,long_row)
    require(staged_commit['memory']['instance_bytes']==base+1301 and staged_commit['memory']['available_bytes']==1,'staged row did not fill the ledger')
    before_refusal=s.stats()
    commit_refusal=rejection(lambda:s.commit(retry_tx))
    after_refusal=s.stats()
    require(commit_refusal['details']['scope']=='instance_bytes' and after_refusal['sequence']==before_refusal['sequence'],'commit refusal advanced durable state')
    require(after_refusal['commits_since_open']==before_refusal['commits_since_open'] and any(t['id']==retry_tx for t in after_refusal['active']),'commit refusal closed the session')
    require(next(t for t in after_refusal['active'] if t['id']==retry_tx)['staged_bytes']==after_refusal['memory']['private_bytes'],'commit refusal lost private edits')
    shrunk_commit=s.put(retry_tx,{'sample':50,'band':'q','amplitude':9999,'valid':True})
    retried=s.commit(retry_tx)
    require(retried['writes']==1 and s.query(where=[{'column':'sample','op':'=','value':50}])['rows'][0]['band']=='q','shrunk commit did not publish')
    s.call('POST','/checkpoint');s.stop();s.start()
    final=s.stats()
    require(not final['active'] and final['memory']['private_bytes']==0,'restart left private charge')
    require(final['memory']['instance_bytes']==final['memory']['version_bytes']+final['memory']['index_bytes'],'final ledger components disagree')
    observation={'limits':{'instance':start['instance_limit'],'transaction':start['transaction_limit']},
      'refused':{'scope':refused['details']['scope'],'message':refused['message'],'preserved_keys':refused['details']['transaction_keys']},
      'after_refusal':{'staged_keys':active['staged_keys'],'staged_bytes':active['staged_bytes'],'read_own_matched':7+2},
      'after_shrink':{'staged_bytes':shrunk['transaction']['staged_bytes']},
      'cross_relation_refused':cross_refused['details']['scope'],'freed_by_tombstone':freed['memory']['private_bytes'],
      'commit':committed,'ledger_after_commit':after_commit,
      'pinned_vacuum':pinned,'reclaimed_vacuum':reclaimed,'ledger_after_vacuum':reduced,
      'restart':{'instance_bytes':restarted['instance_bytes'],'version_bytes':restarted['version_bytes'],'index_bytes':restarted['index_bytes']},
      'tight_transaction_limit':{'limit':tight['details']['transaction_limit'],'scope':tight['details']['scope'],'staged_at_refusal':tight['details']['transaction_bytes'],'requested':tight['details']['requested_bytes'],'retry_commit':tight_commit},
      'commit_refusal':{'available_before':staged_commit['memory']['available_bytes'],'scope':commit_refusal['details']['scope'],'sequence_after_refusal':after_refusal['sequence'],'staged_keys_after_refusal':next(t for t in after_refusal['active'] if t['id']==retry_tx)['staged_keys'],'shrunk_bytes':shrunk_commit['transaction']['staged_bytes'],'retry':retried},
      'final':{'sequence':final['sequence'],'instance_bytes':final['memory']['instance_bytes'],'version_bytes':final['memory']['version_bytes'],'index_bytes':final['memory']['index_bytes'],'capacity_rejections_since_open':final['memory']['capacity_rejections_since_open']},
      # Interleaving checks: the long reader was closed before reclamation, the
      # ledger identity held through commit, and each refusal left the stage
      # exactly as it was.
      'interleave':{'reader_closed_before_reclaim':pinned['pinned'] and not reclaimed['pinned'] and reclaimed['reclaimed_bytes']>0,'ledger_identity':reduced['instance_bytes']==reduced['version_bytes']+reduced['index_bytes']+reduced['private_bytes'],'refusals_without_sequence_advance':[refused['details']['scope'],commit_refusal['details']['scope']],'staged_survived_refusal':active['staged_keys']==2 and next(t for t in after_refusal['active'] if t['id']==retry_tx)['staged_keys']==1}}
    observations=observation
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()
