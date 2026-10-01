/* One renderer for all strategy adapters. No strategy rules or third-party dependencies. */
(() => {
  'use strict';
  const root = document.getElementById('strategy-chart');
  if (!root || root.dataset.initialized) return;
  root.dataset.initialized = 'true';
  const $ = id => document.getElementById(id);
  const form = $('chart-controls'), canvas = $('chart-canvas'), ctx = canvas.getContext('2d');
  const colors = {channel:'#60a5fa',entry:'#34d399',target:'#c084fc',stop:'#f87171',formation:'#fbbf24',average:'#60a5fa',trigger:'#e9b949',position:'#a5b4fc'};
  const ink = {up:'#34d399',down:'#f87171',muted:'#6b7280',grid:'#30363d',text:'#9aa5b1',bg:'#161b22',exit:'#e5e7eb',model:'#fbbf24',intent:'#93c5fd',schedule:'#c084fc'};
  const roleLayers = ['channel','entry','target','stop','formation','average','trigger','position'];
  const markerLayers = {execution:'executions',model:'model events',intent:'intents',diagnostic:'diagnostics',schedule:'schedule'};
  const enabled = new Set([...roleLayers, 'execution', 'model', 'intent', 'schedule']);
  let data = null, viewport = null, follow = true, hover = null, drag = null, abort = null, frame = 0, hoverCandle = null;
  const fmt = n => n == null ? '—' : new Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(n);
  const date = t => t == null ? '—' : new Date(t*1000).toISOString().replace('T',' ').slice(0,16)+' UTC';
  const text = (tag, value, cls) => {const el=document.createElement(tag);el.textContent=value;if(cls)el.className=cls;return el;};
  const params = new URLSearchParams(location.search);
  // MA chooses its decision timeframe; breakout and range always use 4h candles.
  form.strategy.value = ['ma','breakout','range'].includes(params.get('strategy')) ? params.get('strategy') : 'ma';
  form.tf.value = params.get('tf') === '4h' ? '4h' : '1d';
  for (const [key, name] of [...roleLayers.map(r => [r, r]), ...Object.entries(markerLayers)]) {
    const label=text('label',''), input=document.createElement('input');input.type='checkbox';input.checked=enabled.has(key);label.dataset.layer=key;
    input.onchange=()=>{input.checked?enabled.add(key):enabled.delete(key);schedule();levels(true);if(data)events();};
    label.append(input,document.createTextNode(name));$('chart-layers').append(label);
  }
  function query() {
    const strategy=form.strategy.value, tf=strategy==='ma'?form.tf.value:'4h';
    return new URLSearchParams({strategy,tf,ma_tf:tf,days:'90'});
  }
  function saveURL() {
    const u=new URL(location.href);
    for(const k of ['strategy','tf','days','ma_tf','end'])u.searchParams.delete(k);
    u.searchParams.set('strategy',form.strategy.value);if(form.strategy.value==='ma')u.searchParams.set('tf',form.tf.value);
    history.replaceState(null,'',u);
  }
  async function load(reset=false) {
    if(abort)abort.abort();const request=new AbortController();abort=request;
    try {
      const response=await fetch('/api/chart?'+query(),{cache:'no-store',signal:request.signal});
      const value=await response.json();if(!response.ok)throw new Error(value.error||'Chart unavailable');
      if(abort!==request)return;
      prepare(value);data=value;
      if(reset||!viewport){viewport=[data.start,data.end];follow=true;}
      else if(follow){const width=viewport[1]-viewport[0];viewport=[data.end-width,data.end];}
      $('chart-error').textContent='';
      const present=new Set([...data.segments,...data.bands,...data.series].map(s=>s.role));
      for(const m of data.markers)present.add(m.layer);
      for(const label of $('chart-layers').children)label.style.display=present.has(label.dataset.layer)?'':'none';
      $('chart-tf').style.display=form.strategy.value==='ma'?'':'none';
      status();events();schedule();levels(true);
    } catch(e) {if(e.name!=='AbortError')$('chart-error').textContent=e.message;}
    finally {if(abort===request)abort=null;}
  }
  // Index once per payload: candle lookup, step connectors and marker stacking.
  function prepare(view) {
    const byLabel=new Map();
    for(const s of view.segments){if(!byLabel.has(s.label))byLabel.set(s.label,[]);byLabel.get(s.label).push(s);}
    view.risers=[];
    for(const list of byLabel.values()){list.sort((p,q)=>p.start-q.start);
      for(let i=1;i<list.length;i++){const p=list[i-1],q=list[i];if(q.start===p.end&&q.value!==p.value)view.risers.push({role:q.role,label:q.label,time:q.start,from:p.value,to:q.value,eligible:p.eligible&&q.eligible});}}
    const executions=view.markers.filter(m=>m.layer==='execution').map(m=>m.time);
    for(const m of view.markers){
      m.candle=candleAt(view.candles,m.anchor??m.time);
      // An intent followed by its fill is shown once, as the fill.
      m.redundant=m.layer==='intent'&&executions.some(t=>t>=m.time&&t-m.time<=300);
    }
  }
  function candleAt(candles,t) {
    let lo=0,hi=candles.length-1;
    while(lo<=hi){const mid=(lo+hi)>>1,c=candles[mid];if(t<c.time)hi=mid-1;else if(t>=c.close_time)lo=mid+1;else return c;}
    return null;
  }
  // A short, strategy-specific summary of the saved state; full detail lives on /strategies.
  const stateFields=[
    ['phase','Phase'],['admission','Admission'],['channel','Channel',v=>v.split(' to ').map(x=>'$'+fmt(Number(x))).join(' – ')],
    ['confirmation_er','Confirm ER',v=>Number(v).toFixed(3)],['redraws','Redraws'],['expiry_utc','Expires',v=>String(v).slice(0,10)],
    ['campaign','Campaign'],['held_days','Day'],['mode','Mode',v=>String(v).replaceAll('_',' ')],
    ['Recovery condition starts','Recovery from',v=>String(v).slice(0,10)],['Maximum hold','Max hold',v=>String(v).slice(0,10)],
    ['winner_remaining','Winner cooldown'],['loss_remaining','Loss cooldown'],['manual_hold','Manual hold'],['pending_exit','Exit pending'],
  ];
  function status() {
    const dl=$('chart-status');dl.replaceChildren();
    const st=data.status;
    for(const [key,label,format] of stateFields){
      const v=st[key];
      if(v==null||v===''||v===false)continue;
      if(key==='mode'&&data.strategy==='range')continue;
      pair(label,v===true?'yes':format?format(v):String(v));
    }
    if(data.state_as_of)pair('Saved',date(data.state_as_of).slice(5,16));
    function pair(label,value){const item=document.createElement('div');item.append(text('dt',label),text('dd',value));dl.append(item);}
  }
  function levels(force=false) {
    if(!data||!viewport)return;
    const t=Math.min(hover??viewport[1],data.end)-1, c=candleAt(data.candles,t), key=c?c.time:t;
    if(!force&&key===hoverCandle)return;hoverCandle=key;
    $('chart-levels-time').textContent=date(c?c.time:t).slice(5,16);
    const rows=[],seen=new Set();
    for(const s of data.segments){if(!enabled.has(s.role)||s.start>t||s.end<=t||seen.has(s.label))continue;seen.add(s.label);rows.push([s.label,s.value,stroke(s),!s.eligible,s.origin]);}
    for(const s of data.series){if(!enabled.has(s.role)||seen.has(s.label))continue;const p=s.points.filter(p=>p.time<=t).at(-1);if(!p||t-p.time>s.period)continue;
      seen.add(s.label);rows.push([s.label,p.value,s.label.includes('EMA')?colors.target:colors[s.role]||colors.average,false,s.origin]);}
    // Highest price first, like the axis; rows are fixed-height so hover never reflows the page.
    rows.sort((a,b)=>b[1]-a[1]);
    const el=$('chart-levels');el.replaceChildren();
    for(const [label,value,col,muted,origin] of rows){
      const row=text('div','','chart-level'+(muted?' muted-level':''));row.title=label+' · '+origin+(muted?' · entries skipped or paused':'');
      const swatch=text('i','');swatch.style.borderColor=col;
      row.append(swatch,text('span',label),text('b','$'+fmt(value)));el.append(row);
    }
    if(!rows.length)el.append(text('p','No levels at this time.','muted'));
  }
  function events() {
    const el=$('chart-events');el.replaceChildren();
    const shown=[...data.markers].filter(m=>enabled.has(m.layer)&&!m.redundant).reverse();
    for(const m of shown) {
      const row=document.createElement('tr');row.tabIndex=0;row.title=m.detail||'';
      const jump=()=>{follow=false;const width=viewport[1]-viewport[0];viewport=[m.time-width*.65,m.time+width*.35];hover=m.anchor??m.time;schedule();levels();};
      row.onclick=jump;row.onkeydown=e=>{if(e.key==='Enter')jump();};
      const name=m.label.replaceAll('_',' ');
      row.append(text('td',date(m.time).slice(5,16)),text('td',name.charAt(0).toUpperCase()+name.slice(1)),text('td',m.slot||'—'),text('td',m.price==null?'—':'$'+fmt(m.price)),text('td',m.origin));
      el.append(row);
    }
    if(!shown.length){const row=document.createElement('tr'),cell=text('td','No events in this window.','muted');cell.colSpan=5;row.append(cell);el.append(row);}
  }
  function schedule(){if(!frame)frame=requestAnimationFrame(()=>{frame=0;draw();});}
  function dash(s){return !s.eligible?[5,4]:s.role==='stop'||/threshold|close/i.test(s.label)?[5,4]:s.role==='target'?[2,3]:s.role==='trigger'?[2,3]:[];}
  function stroke(s){return s.eligible?colors[s.role]||ink.text:ink.muted;}
  function draw() {
    if(!data||!viewport||!ctx)return;
    const W=canvas.clientWidth,H=canvas.clientHeight,dpr=devicePixelRatio||1;
    if(canvas.width!==Math.round(W*dpr)||canvas.height!==Math.round(H*dpr)){canvas.width=Math.round(W*dpr);canvas.height=Math.round(H*dpr);}
    ctx.setTransform(dpr,0,0,dpr,0,0);
    const pad={l:14,r:96,t:22,b:32},pw=W-pad.l-pad.r,ph=H-pad.t-pad.b;
    if(pw<=0||ph<=0)return;
    const [a,b]=viewport,x=t=>pad.l+(t-a)/(b-a)*pw;
    const visible=data.candles.filter(c=>c.close_time>=a&&c.time<=b);
    // Scale to price first; levels widen it only when near price, so a distant
    // width cap or old boundary cannot squash the candles.
    let low=Infinity,high=-Infinity;for(const c of visible){low=Math.min(low,c.low);high=Math.max(high,c.high);}
    const levelValues=[];
    for(const s of data.segments)if(enabled.has(s.role)&&s.end>a&&s.start<b)levelValues.push(s.value);
    for(const s of data.series)if(enabled.has(s.role))for(const p of s.points)if(p.time>=a&&p.time<=b)levelValues.push(p.value);
    if(!isFinite(low)){for(const v of levelValues){low=Math.min(low,v);high=Math.max(high,v);}}
    else{const reach=Math.max(high-low,high*.01)*.6;for(const v of levelValues)if(v>=low-reach&&v<=high+reach){low=Math.min(low,v);high=Math.max(high,v);}}
    ctx.fillStyle=ink.bg;ctx.fillRect(0,0,W,H);
    if(!isFinite(low)){ctx.fillStyle=ink.text;ctx.font='12px system-ui';ctx.fillText('No recorded price or levels in this window.',20,40);draw.geo=null;return;}
    const margin=Math.max((high-low)*.07,high*.0005);low-=margin;high+=margin;
    const y=v=>pad.t+(high-v)/(high-low)*ph;
    ctx.font='11px system-ui';ctx.lineWidth=1;
    const step=Math.pow(10,Math.floor(Math.log10((high-low)/5))),tick=[1,2,5,10].map(m=>m*step).find(s=>(high-low)/s<=7);
    const ticks=[];for(let v=Math.ceil(low/tick)*tick;v<high;v+=tick){const yy=y(v);ticks.push([v,yy]);ctx.strokeStyle=ink.grid;ctx.beginPath();ctx.moveTo(pad.l,yy);ctx.lineTo(W-pad.r,yy);ctx.stroke();}
    const n=pw<420?2:4,label=t=>b-a>3*86400?date(t).slice(5,10):date(t).slice(5,16);for(let i=0;i<=n;i++){const t=a+(b-a)*i/n;ctx.fillStyle=ink.text;ctx.textAlign=i===0?'left':i===n?'right':'center';ctx.fillText(label(t),x(t),H-10);}ctx.textAlign='left';
    ctx.save();ctx.beginPath();ctx.rect(pad.l,pad.t,pw,ph);ctx.clip();
    for(const band of data.bands){if(!enabled.has(band.role)||band.end<a||band.start>b)continue;ctx.fillStyle=band.eligible?(colors[band.role]||ink.up):ink.text;ctx.globalAlpha=band.eligible?.1:.06;ctx.fillRect(x(band.start),y(band.upper),x(band.end)-x(band.start),y(band.lower)-y(band.upper));}
    ctx.globalAlpha=1;
    const slot=visible.length?Math.max(1,(x(visible[0].close_time)-x(visible[0].time))):1;
    for(const c of visible){
      const live=c.close_time>data.end, period=c.close_time-c.time;
      const cx=x(c.time+period/2),width=Math.max(1,Math.min(16,slot*.65));
      const sparse=!live&&c.missing_minutes>period/600;
      const col=c.close>=c.open?ink.up:ink.down;ctx.strokeStyle=sparse?ink.model:col;ctx.fillStyle=col;ctx.globalAlpha=live?.6:1;
      ctx.beginPath();ctx.moveTo(cx,y(c.high));ctx.lineTo(cx,y(c.low));ctx.stroke();
      const top=y(Math.max(c.open,c.close)),height=Math.max(1,Math.abs(y(c.open)-y(c.close)));
      ctx.fillRect(cx-width/2,top,width,height);if(sparse)ctx.strokeRect(cx-width/2,top,width,height);
    }
    ctx.globalAlpha=1;ctx.lineWidth=1.25;
    for(const s of data.segments){if(!enabled.has(s.role)||s.end<a||s.start>b)continue;ctx.strokeStyle=stroke(s);ctx.setLineDash(dash(s));ctx.beginPath();ctx.moveTo(x(s.start),y(s.value));ctx.lineTo(x(s.end),y(s.value));ctx.stroke();}
    for(const r of data.risers){if(!enabled.has(r.role)||r.time<a||r.time>b)continue;ctx.strokeStyle=stroke(r);ctx.setLineDash(dash(r));ctx.beginPath();ctx.moveTo(x(r.time),y(r.from));ctx.lineTo(x(r.time),y(r.to));ctx.stroke();}
    for(const s of data.series){if(!enabled.has(s.role))continue;ctx.strokeStyle=s.label.includes('EMA')?colors.target:colors[s.role]||colors.average;ctx.setLineDash(s.role==='trigger'?[2,3]:[]);ctx.lineWidth=s.role==='trigger'?1:1.5;ctx.beginPath();let previous=null;
      for(const p of s.points){if(p.time<a-s.period||p.time>b+s.period)continue;if(previous==null||p.time-previous>s.period*1.5)ctx.moveTo(x(p.time),y(p.value));else ctx.lineTo(x(p.time),y(p.value));previous=p.time;}ctx.stroke();}
    ctx.setLineDash([]);ctx.lineWidth=1;
    const hits=[];
    if(enabled.has('schedule'))for(const m of data.markers){if(m.layer!=='schedule'||m.time<a||m.time>b)continue;const xx=x(m.time);
      ctx.strokeStyle=ink.schedule;ctx.setLineDash([2,4]);ctx.beginPath();ctx.moveTo(xx,pad.t);ctx.lineTo(xx,H-pad.b);ctx.stroke();ctx.setLineDash([]);
      ctx.fillStyle=ink.schedule;ctx.fillText(m.label,xx+4,pad.t+10);hits.push({x:xx,y:pad.t+8,m});}
    const above=new Map(),below=new Map();
    for(const m of data.markers){
      if(m.layer==='schedule'||!enabled.has(m.layer)||m.redundant)continue;
      const c=m.candle;const t=c?c.time+(c.close_time-c.time)/2:m.anchor??m.time;if(t<a||t>b)continue;
      const xx=x(t);let yy;
      if(m.price!=null&&m.layer!=='diagnostic')yy=y(m.price);
      else if(c){const store=m.layer==='diagnostic'?below:above,n=store.get(c.time)||0;store.set(c.time,n+1);
        yy=m.layer==='diagnostic'?y(c.low)+9+n*9:y(c.high)-9-n*11;}
      else continue;
      shape(m,xx,yy);hits.push({x:xx,y:yy,m});
    }
    if(hover!=null){ctx.strokeStyle=ink.text;ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(x(hover),pad.t);ctx.lineTo(x(hover),H-pad.b);ctx.stroke();ctx.setLineDash([]);}
    ctx.restore();
    const used=tags(x,y,a,b,W,pad,ph);ctx.fillStyle=ink.text;
    for(const [v,yy] of ticks)if(!used.some(u=>Math.abs(u-yy)<12))ctx.fillText(fmt(v),W-pad.r+7,yy+4);
    canvas.dataset.windowStart=String(a);canvas.dataset.windowEnd=String(b);
    draw.geo={a,b,pw,pad,hits};
  }
  function shape(m,xx,yy) {
    const d=m.direction, model=m.kind.startsWith('shadow_')||m.kind==='model_entry';
    ctx.lineWidth=1.5;
    if(m.kind==='entry'||m.kind==='shadow_entry'||m.kind==='model_entry'){
      // Tip on the fill price: ▲ long below it, ▼ short above it.
      const s=d===-1?-1:1,col=model?ink.model:s===1?ink.up:ink.down;
      ctx.beginPath();ctx.moveTo(xx,yy);ctx.lineTo(xx-6,yy+10*s);ctx.lineTo(xx+6,yy+10*s);ctx.closePath();
      if(model){ctx.strokeStyle=col;ctx.stroke();}else{ctx.fillStyle=col;ctx.strokeStyle=ink.bg;ctx.stroke();ctx.fill();}
    } else if(m.kind==='exit'||m.kind==='shadow_exit'||m.kind==='model_exit'){
      ctx.beginPath();ctx.arc(xx,yy,4.5,0,Math.PI*2);
      if(model||m.kind==='model_exit'){ctx.strokeStyle=ink.model;ctx.stroke();}
      else{ctx.fillStyle=ink.exit;ctx.fill();ctx.strokeStyle=d===-1?ink.down:ink.up;ctx.stroke();}
    } else if(m.layer==='model'){
      ctx.beginPath();ctx.moveTo(xx,yy-5);ctx.lineTo(xx+5,yy);ctx.lineTo(xx,yy+5);ctx.lineTo(xx-5,yy);ctx.closePath();ctx.fillStyle=ink.model;ctx.strokeStyle=ink.bg;ctx.stroke();ctx.fill();
    } else if(m.layer==='intent'){
      ctx.beginPath();ctx.arc(xx,yy,3.5,0,Math.PI*2);ctx.strokeStyle=ink.intent;ctx.stroke();
    } else {
      ctx.beginPath();ctx.arc(xx,yy,2,0,Math.PI*2);ctx.fillStyle=ink.muted;ctx.fill();
    }
    ctx.lineWidth=1;
  }
  // Right-axis tags for levels in force at the right edge, as in the research replay.
  function tags(x,y,a,b,W,pad,ph) {
    const t=Math.min(b,data.end)-1,seen=new Set(),items=[];
    for(const s of data.segments)if(enabled.has(s.role)&&s.start<=t&&s.end>t&&!seen.has(s.label)){seen.add(s.label);items.push([s.value,stroke(s)]);}
    for(const s of data.series){if(!enabled.has(s.role)||s.role==='trigger'||seen.has(s.label))continue;const p=s.points.filter(p=>p.time<=t).at(-1);if(p&&t-p.time<=s.period)items.push([p.value,s.label.includes('EMA')?colors.target:colors.average]);}
    const last=data.candles.at(-1);if(last&&last.close_time>a)items.push([last.close,'#e5e7eb']);
    items.sort((p,q)=>q[0]-p[0]);const used=[];ctx.font='600 10px system-ui';
    for(const [value,col] of items){let yy=y(value);if(yy<pad.t||yy>pad.t+ph)continue;while(used.some(u=>Math.abs(u-yy)<12))yy+=12;used.push(yy);
      ctx.fillStyle=col;ctx.fillRect(W-pad.r+2,yy-6,pad.r-4,12);ctx.fillStyle=ink.bg;ctx.fillText(fmt(value),W-pad.r+6,yy+4);}
    ctx.font='11px system-ui';return used;
  }
  function timeAt(event){const g=draw.geo,r=canvas.getBoundingClientRect();return g?g.a+(event.clientX-r.left-g.pad.l)/g.pw*(g.b-g.a):null;}
  canvas.addEventListener('wheel',e=>{if(!draw.geo)return;e.preventDefault();const t=timeAt(e),width=viewport[1]-viewport[0],next=Math.max(3600,Math.min(90*86400,width*(e.deltaY>0?1.2:.8))),ratio=(t-viewport[0])/width;viewport=[t-next*ratio,t+next*(1-ratio)];follow=false;schedule();levels();},{passive:false});
  canvas.addEventListener('pointerdown',e=>{if(!draw.geo)return;drag={x:e.clientX,viewport:[...viewport]};canvas.setPointerCapture(e.pointerId);});
  canvas.addEventListener('pointerup',()=>{drag=null;});canvas.addEventListener('pointercancel',()=>{drag=null;});
  canvas.addEventListener('pointermove',e=>{
    if(!draw.geo)return;
    if(drag){const shift=(e.clientX-drag.x)/draw.geo.pw*(drag.viewport[1]-drag.viewport[0]);viewport=[drag.viewport[0]-shift,drag.viewport[1]-shift];follow=false;schedule();return;}
    hover=timeAt(e);if(hover==null)return;schedule();levels();
    const r=canvas.getBoundingClientRect(),px=e.clientX-r.left,py=e.clientY-r.top;
    const c=candleAt(data.candles,hover),tip=$('chart-tooltip');tip.hidden=false;
    const near=draw.geo.hits.filter(h=>Math.abs(h.x-px)<=8&&Math.abs(h.y-py)<=10).map(h=>h.m);
    const nearby=near.length?near:draw.geo.hits.filter(h=>c&&h.m.candle===c).map(h=>h.m);
    tip.textContent=(c?`${date(c.time)}\nO ${fmt(c.open)} · H ${fmt(c.high)} · L ${fmt(c.low)} · C ${fmt(c.close)}`+(c.missing_minutes&&c.close_time<=data.end?`\n${c.missing_minutes} missing minute${c.missing_minutes>1?'s':''}`:''):date(hover)+'\nNo recorded candle')
      +nearby.slice(0,5).map(m=>`\n${m.label}${m.price==null?'':' · $'+fmt(m.price)} · ${m.origin}`).join('');
  });
  canvas.addEventListener('pointerleave',()=>{hover=null;$('chart-tooltip').hidden=true;schedule();levels();});
  canvas.addEventListener('keydown',e=>{if(!viewport)return;if(['ArrowLeft','ArrowRight'].includes(e.key)){e.preventDefault();const shift=(viewport[1]-viewport[0])*.1*(e.key==='ArrowLeft'?-1:1);viewport=viewport.map(v=>v+shift);follow=false;schedule();levels();}});
  form.addEventListener('change',()=>{saveURL();load(true);});
  form.addEventListener('submit',e=>e.preventDefault());
  $('chart-latest').onclick=()=>{hover=null;load(true);};
  new ResizeObserver(()=>schedule()).observe(canvas.parentElement);
  // Prices advance once a minute; the server cache answers repeats cheaply.
  setInterval(()=>{if(!document.hidden&&!abort&&!drag)load();},15000);
  load(true);
})();
