/* One renderer for all strategy adapters. No strategy rules or third-party dependencies. */
(() => {
  'use strict';
  const root = document.getElementById('strategy-chart');
  if (!root || root.dataset.initialized) return;
  root.dataset.initialized = 'true';
  const $ = id => document.getElementById(id);
  const form = $('chart-controls'), canvas = $('chart-canvas'), ctx = canvas.getContext('2d');
  const colors = {channel:'#60a5fa',entry:'#34d399',target:'#c084fc',stop:'#f87171',formation:'#fbbf24',average:'#60a5fa',trigger:'#e9b949',position:'#a5b4fc'};
  const roles = ['channel','entry','target','stop','formation','average','trigger','position','events'];
  const enabled = new Set(roles);
  let data = null, viewport = null, follow = true, hover = null, drag = null, abort = null;
  const fmt = n => n == null ? '—' : new Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(n);
  const date = t => t == null ? '—' : new Date(t*1000).toISOString().replace('T',' ').slice(0,16)+' UTC';
  const text = (tag, value, cls) => {const el=document.createElement(tag);el.textContent=value;if(cls)el.className=cls;return el;};
  const params = new URLSearchParams(location.search);
  form.strategy.value = params.get('strategy') || 'ma';
  form.tf.value = params.get('tf') || (form.strategy.value==='range'?'4h':'1d');
  form.ma_tf.value = params.get('ma_tf') || (form.tf.value==='1m'?'4h':form.tf.value);
  form.days.value = params.get('days') || (form.tf.value==='1m'?'1':'30');
  let end = params.get('end');
  for (const role of roles) {
    const label=text('label',''), input=document.createElement('input');input.type='checkbox';input.checked=true;
    input.onchange=()=>{input.checked?enabled.add(role):enabled.delete(role);draw();levels();};
    label.append(input,document.createTextNode(role));$('chart-layers').append(label);
  }
  function query() {
    const q=new URLSearchParams(new FormData(form));if(end)q.set('end',end);return q;
  }
  function saveURL() {
    const u=new URL(location.href);
    for(const k of ['strategy','tf','days','ma_tf','end'])u.searchParams.delete(k);
    for(const [k,v] of query())u.searchParams.set(k,v);
    history.replaceState(null,'',u);
  }
  async function load(reset=false) {
    if(abort)abort.abort();const request=new AbortController();abort=request;
    try {
      const response=await fetch('/api/chart?'+query(),{cache:'no-store',signal:request.signal});
      const value=await response.json();if(!response.ok)throw new Error(value.error||'Chart unavailable');
      if(abort!==request)return;
      data=value;
      if(reset||!viewport){viewport=[data.start,data.end];follow=true;}
      else if(follow){const width=viewport[1]-viewport[0];viewport=[data.end-width,data.end];}
      $('chart-error').textContent='';
      const present=new Set([...data.segments,...data.bands,...data.series].map(s=>s.role));present.add('events');
      for(const label of $('chart-layers').children)label.style.display=present.has(label.lastChild.textContent)?'':'none';
      form.ma_tf.parentElement.style.display=form.strategy.value==='ma'?'':'none';
      status();events();draw();levels();
    } catch(e) {if(e.name!=='AbortError')$('chart-error').textContent=e.message;}
    finally {if(abort===request)abort=null;}
  }
  function status() {
    const dl=$('chart-status');dl.replaceChildren();
    const rows={...data.status,'State as of':date(data.state_as_of),'Latest recorded minute':date(data.candles_as_of),'Owner':data.instance_id,'Run':data.run_id,'Rule version':data.rule_version};
    for(const [k,v] of Object.entries(rows)){if(v==null||v==='')continue;dl.append(text('dt',k.replaceAll('_',' ')),text('dd',String(v)));}
    $('chart-coverage').textContent=`${data.candles.length} candles · ${data.coverage.missing_minutes.toLocaleString()} missing recorded minutes · partial candles outlined amber. Record gaps do not diagnose live model health.`;
    const ul=$('chart-warnings');ul.replaceChildren();for(const warning of data.coverage.warnings)ul.append(text('li',warning));
  }
  function levels() {
    if(!data||!viewport)return;
    const el=$('chart-levels');el.replaceChildren();
    const t=hover??Math.min(viewport[1],data.end)-1, seen=new Set();
    for(const s of data.segments){if(!enabled.has(s.role)||s.start>t||s.end<=t||seen.has(s.label))continue;seen.add(s.label);
      const row=text('div',`${s.label}: $${fmt(s.value)}`,'chart-level');row.append(text('small',s.origin+(s.eligible?'':' · entries skipped')));el.append(row);}
    for(const s of data.series){if(!enabled.has(s.role))continue;const p=s.points.filter(p=>p.time<=t).at(-1);if(!p||t-p.time>s.period)continue;
      const row=text('div',`${s.label}: $${fmt(p.value)}`,'chart-level');row.append(text('small',s.origin));el.append(row);}
    if(!el.childNodes.length)el.append(text('p','No recorded levels at this time.','muted'));
  }
  function events() {
    const el=$('chart-events');el.replaceChildren();
    for(const m of [...data.markers].reverse()) {
      const row=text('div','','chart-event'), jump=text('button',date(m.time));jump.type='button';
      jump.onclick=()=>{follow=false;const width=viewport[1]-viewport[0];viewport=[m.time-width*.65,m.time+width*.35];hover=m.time;draw();levels();};
      const info=text('div',`${m.label}${m.slot?' · '+m.slot:''}${m.price==null?'':' · $'+fmt(m.price)}`,'chart-event-description');
      info.append(text('small',m.origin+(m.detail?' · '+m.detail:'')));
      const zoom=text('button','1m inspection');zoom.type='button';zoom.onclick=()=>{end=new Date((m.time+6*3600)*1000).toISOString();form.tf.value='1m';form.days.value='1';saveURL();load(true);};
      row.append(jump,info,zoom);el.append(row);
    }
    if(!el.childNodes.length)el.append(text('p','No recorded events in this window.','muted'));
  }
  function draw() {
    if(!data||!viewport||!ctx)return;
    const W=canvas.clientWidth,H=canvas.clientHeight,dpr=devicePixelRatio||1;
    canvas.width=W*dpr;canvas.height=H*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);
    const pad={l:14,r:92,t:22,b:32},pw=W-pad.l-pad.r,ph=H-pad.t-pad.b;
    if(pw<=0||ph<=0)return;
    const [a,b]=viewport,x=t=>pad.l+(t-a)/(b-a)*pw;
    const visible=data.candles.filter(c=>c.close_time>=a&&c.time<=b);
    const values=visible.flatMap(c=>[c.low,c.high]);
    for(const s of data.segments)if(enabled.has(s.role)&&s.end>a&&s.start<b)values.push(s.value);
    for(const s of data.series)if(enabled.has(s.role))for(const p of s.points)if(p.time>=a&&p.time<=b)values.push(p.value);
    if(!values.length){ctx.clearRect(0,0,W,H);ctx.fillStyle='#9aa5b1';ctx.fillText('No recorded price or levels in this window.',20,40);return;}
    let low=Math.min(...values),high=Math.max(...values);const margin=Math.max((high-low)*.07,high*.0005);low-=margin;high+=margin;
    const y=v=>pad.t+(high-v)/(high-low)*ph;
    ctx.fillStyle='#161b22';ctx.fillRect(0,0,W,H);ctx.font='11px system-ui';ctx.lineWidth=1;
    for(let i=0;i<=5;i++){const v=low+(high-low)*i/5,yy=y(v);ctx.strokeStyle='#30363d';ctx.beginPath();ctx.moveTo(pad.l,yy);ctx.lineTo(W-pad.r,yy);ctx.stroke();ctx.fillStyle='#9aa5b1';ctx.fillText(fmt(v),W-pad.r+7,yy+4);}
    for(let i=0;i<=4;i++){const t=a+(b-a)*i/4;ctx.fillStyle='#9aa5b1';ctx.textAlign=i===0?'left':i===4?'right':'center';ctx.fillText(date(t).slice(5,16),x(t),H-10);}ctx.textAlign='left';
    ctx.save();ctx.beginPath();ctx.rect(pad.l,pad.t,pw,ph);ctx.clip();
    for(const band of data.bands){if(!enabled.has(band.role)||band.end<a||band.start>b)continue;ctx.fillStyle=band.eligible?(colors[band.role]||'#34d399'):'#9aa5b1';ctx.globalAlpha=.09;ctx.fillRect(x(band.start),y(band.upper),x(band.end)-x(band.start),y(band.lower)-y(band.upper));ctx.globalAlpha=1;}
    for(const c of visible){const cx=x((c.time+Math.min(c.close_time,data.end))/2),width=Math.max(1,Math.min(16,(x(c.close_time)-x(c.time))*.65));
      const col=c.complete?(c.close>=c.open?'#34d399':'#f87171'):'#fbbf24';ctx.strokeStyle=col;ctx.fillStyle=col;
      ctx.beginPath();ctx.moveTo(cx,y(c.high));ctx.lineTo(cx,y(c.low));ctx.stroke();
      const top=y(Math.max(c.open,c.close)),height=Math.max(1,Math.abs(y(c.open)-y(c.close)));
      c.complete?ctx.fillRect(cx-width/2,top,width,height):ctx.strokeRect(cx-width/2,top,width,height);
    }
    for(const s of data.segments){if(!enabled.has(s.role)||s.end<a||s.start>b)continue;ctx.strokeStyle=s.eligible?colors[s.role]||'#9aa5b1':'#9aa5b1';ctx.lineWidth=1.25;ctx.setLineDash(s.role==='stop'||!s.eligible?[5,4]:s.role==='trigger'?[2,3]:[]);ctx.beginPath();ctx.moveTo(x(s.start),y(s.value));ctx.lineTo(x(s.end),y(s.value));ctx.stroke();}
    ctx.setLineDash([]);
    for(const s of data.series){if(!enabled.has(s.role))continue;ctx.strokeStyle=colors[s.role]||'#60a5fa';if(s.label.includes('EMA'))ctx.strokeStyle='#c084fc';ctx.lineWidth=1.5;ctx.beginPath();let previous=null;
      for(const p of s.points){if(p.time<a-s.period||p.time>b+s.period)continue;if(previous==null||p.time-previous>s.period)ctx.moveTo(x(p.time),y(p.value));else ctx.lineTo(x(p.time),y(p.value));previous=p.time;}ctx.stroke();}
    if(enabled.has('events'))for(const m of data.markers){if(m.time<a||m.time>b)continue;const xx=x(m.time),yy=m.price==null?pad.t+8:y(m.price);ctx.fillStyle=m.kind==='entry'?'#34d399':m.kind==='exit'?'#f87171':'#fbbf24';
      ctx.beginPath();if(['entry','exit'].includes(m.kind)){ctx.moveTo(xx,yy-5);ctx.lineTo(xx+5,yy+5);ctx.lineTo(xx-5,yy+5);ctx.closePath();ctx.fill();}else{ctx.arc(xx,yy,3,0,Math.PI*2);ctx.strokeStyle=ctx.fillStyle;ctx.stroke();}}
    if(hover!=null){ctx.strokeStyle='#9aa5b1';ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(x(hover),pad.t);ctx.lineTo(x(hover),H-pad.b);ctx.stroke();ctx.setLineDash([]);}ctx.restore();
    canvas.dataset.windowStart=String(a);canvas.dataset.windowEnd=String(b);
    draw.geo={a,b,pw,pad};
  }
  function timeAt(event){const g=draw.geo,r=canvas.getBoundingClientRect();return g?g.a+(event.clientX-r.left-g.pad.l)/g.pw*(g.b-g.a):null;}
  canvas.addEventListener('wheel',e=>{if(!draw.geo)return;e.preventDefault();const t=timeAt(e),width=viewport[1]-viewport[0],next=Math.max(3600,Math.min(90*86400,width*(e.deltaY>0?1.2:.8))),ratio=(t-viewport[0])/width;viewport=[t-next*ratio,t+next*(1-ratio)];follow=false;draw();levels();},{passive:false});
  canvas.addEventListener('pointerdown',e=>{if(!draw.geo)return;drag={x:e.clientX,viewport:[...viewport]};canvas.setPointerCapture(e.pointerId);});
  canvas.addEventListener('pointerup',()=>{drag=null;});canvas.addEventListener('pointercancel',()=>{drag=null;});
  canvas.addEventListener('pointermove',e=>{
    if(drag){const shift=(e.clientX-drag.x)/draw.geo.pw*(drag.viewport[1]-drag.viewport[0]);viewport=[drag.viewport[0]-shift,drag.viewport[1]-shift];follow=false;draw();return;}
    hover=timeAt(e);if(hover==null)return;draw();levels();const c=data.candles.find(c=>c.time<=hover&&c.close_time>hover),tip=$('chart-tooltip');tip.hidden=false;
    const nearest=data.markers.filter(m=>Math.abs(m.time-hover)<(viewport[1]-viewport[0])*.008);
    tip.textContent=date(hover)+(c?`\nO ${fmt(c.open)} · H ${fmt(c.high)} · L ${fmt(c.low)} · C ${fmt(c.close)}\n${c.complete?'Complete':c.missing_minutes+' missing minutes / partial'}`:'\nNo recorded candle')+nearest.slice(0,4).map(m=>'\n'+m.label+' · '+m.origin).join('');
  });
  canvas.addEventListener('pointerleave',()=>{hover=null;$('chart-tooltip').hidden=true;draw();levels();});
  canvas.addEventListener('keydown',e=>{if(!viewport)return;if(['ArrowLeft','ArrowRight'].includes(e.key)){e.preventDefault();const shift=(viewport[1]-viewport[0])*.1*(e.key==='ArrowLeft'?-1:1);viewport=viewport.map(v=>v+shift);follow=false;draw();levels();}});
  form.addEventListener('change',e=>{if(e.target.name==='strategy'){form.tf.value=form.strategy.value==='range'?'4h':'1d';form.ma_tf.value=form.tf.value;}if(form.tf.value==='1m')form.days.value='1';else if(e.target.name==='tf')form.ma_tf.value=form.tf.value;end=null;saveURL();load(true);});
  form.addEventListener('submit',e=>e.preventDefault());
  $('chart-latest').onclick=()=>{end=null;saveURL();load(true);};
  new ResizeObserver(()=>draw()).observe(canvas.parentElement);
  setInterval(()=>{if(!document.hidden&&!abort)load();},10000);
  load(true);
})();
