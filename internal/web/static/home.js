// Ausgelagert aus gui/templates/home.html (grosser Inline-<script>): JS-Template-
// Literals (${...}) + Go-Actions im selben Block brechen Go html/template, daher
// hier statisch. Server-Werte kommen aus window.GRAPH_LINKS / window.NETWORK /
// window.NETWORK_LINKS (in base.html gesetzt). {% csrf_token %} entfernt (CSRF aus).
  const now = new Date(),
    _1day_ago = new Date(new Date().setDate(now.getDate()-1)).getTime(),
    _7days_ago = new Date(new Date().setDate(now.getDate()-7)).toISOString().substring(0,19)

  const home_ch_base = Object.assign({}, base_ch_template, {
    "unsettled": ch => (ch.unsettled_balance > 0 ? {innerHTML: `${ch.unsettled_balance.intcomma()} <small class="w3-round w3-border-small w3-border-grey">${ch.htlc_count}</small>`} : {innerHTML: `${ch.unsettled_balance.intcomma()}`, style: {color: 'gray'} }),
    "oRate": ch => ({innerHTML: `<span class="input" style="min-width:100px;max-width:120px;width:100%" data-unit="ₚₚₘ"><input style="text-align:left;width:100%" type="number" min="0" size="6" max="100000" value="${ch.local_fee_rate}" onkeydown="event.keyCode != 13? null : (Sync.POST('chanpolicy/', {body: {chan_id:'${ch.chan_id}', fee_rate: this.value}}, r => {flash(this, r.fee_rate);showBannerMsg('Out Fee Rate', r.fee_rate)}));"/></span>`, style: {backgroundColor: ch.local_disabled ? red : null} }),
    "oBase": ch => ({innerHTML: ch.local_base_fee.intcomma(), style: {backgroundColor: ch.local_disabled ? red : null} }),
  })
  const detailed_ch_template = Object.assign({}, home_ch_base, {
    "o1D": ch => ({innerHTML: `${(ch.amt_routed_out_1day/1000000).toFixed(2)}M <small class="w3-round w3-border-small w3-border-grey">${ch.routed_out_1day}</small>`, style: {color: ch.routed_out_1day ? `rgba(${(100 + (255*ch.amt_routed_out_1day/ch.amt_routed_out_7day))*(darkMode? 1 : .7)},100,0, ${.5 + (ch.amt_routed_out_1day/ch.amt_routed_out_7day)})` : 'gray'} }),
    "i1D": ch => ({innerHTML: `${(ch.amt_routed_in_1day/1000000).toFixed(2)}M <small class="w3-round w3-border-small w3-border-grey">${ch.routed_in_1day}</small>`, style: {color: ch.routed_in_1day ? `rgba(100,${(100 + (255*ch.amt_routed_in_1day/ch.amt_routed_in_7day))*(darkMode? 1 : .7)},0, ${.5 + (ch.amt_routed_in_1day/ch.amt_routed_in_7day)})` : 'gray'} }),
    "o7D": ch => ({innerHTML: `${(ch.amt_routed_out_7day/1000000).toFixed(2)}M <small class="w3-round w3-border-small w3-border-grey">${ch.routed_out_7day}</small>`, style: {color: ch.routed_out_7day ? `rgba(${(100 + (255*ch.amt_routed_out_7day/ch.capacity))*(darkMode? 1 : .7)},100,0, ${.5 + (ch.amt_routed_out_7day/ch.capacity)})` : 'gray'} }),
    "i7D": ch => ({innerHTML: `${(ch.amt_routed_in_7day/1000000).toFixed(2)}M <small class="w3-round w3-border-small w3-border-grey">${ch.routed_in_7day}</small>`, style: {color: ch.routed_in_7day ? `rgba(100,${(100 + (255*ch.amt_routed_in_7day/ch.capacity))*(darkMode? 1 : .7)},0, ${.5 + (ch.amt_routed_in_7day/ch.capacity)})` : 'gray'} }),
    "iRate": ch => ({innerHTML: ch.remote_fee_rate.intcomma(), style: {backgroundColor: ch.remote_disabled ? red : null} }),
    "iBase": ch => ({innerHTML: ch.remote_base_fee.intcomma(), style: {backgroundColor: ch.remote_disabled ? red : null} }),
    "ar_out_target": ch => ({innerHTML: `<input inputmode="numeric" style="min-width:50px;max-width:75px;text-align:center;width:100%" id="target" type="text" min="1" max="100" name="target" value="${ch.ar_out_target}" onChange="Sync.PUT('channels/${ch.chan_id}', {body: {ar_out_target: this.value}}, r => {flash(this, r.ar_out_target)});">`,  style: !ch.auto_rebalance ? {backgroundColor: green} : {color: 'gray', backgroundColor:''} }),
    "ar_in_target": ch => ({innerHTML: `<input inputmode="numeric" style="min-width:50px;max-width:75px;text-align:center;width:100%" id="target" type="text" min="1" max="100" name="target" value="${ch.ar_in_target}" onChange="Sync.PUT('channels/${ch.chan_id}', {body: {ar_in_target: this.value}}, r => {flash(this, r.ar_in_target)});">`, style: ch.auto_rebalance ? {backgroundColor: green} : {color: 'gray', backgroundColor:''} }),
    "auto_rebalance": ch => ({innerHTML: `<input type="submit" value="${ch.auto_rebalance ? 'Dis' : 'En'}able" onclick="Sync.PUT('channels/${ch.chan_id}', {body:{auto_rebalance: ${!ch.auto_rebalance} }}, ch => this.parentElement.changed(ch))">`, style: {backgroundColor: ch.auto_rebalance ? green : null},
      changed: function(newVal) {
        ['ar_in_target', 'auto_rebalance'].forEach(col => this.parentElement.cells[col].render(detailed_ch_template[col](newVal))),
        ['ar_out_target', 'auto_rebalance'].forEach(col => this.parentElement.cells[col].render(detailed_ch_template[col](newVal)))
      }
    }),
  })
  let {iRate, iBase, ar_in_target, auto_rebalance} = detailed_ch_template
  const inactive_ch_template = Object.assign({}, home_ch_base, {iRate, iBase}, {
    "local_commit": ch => ({innerHTML: ch.local_commit.intcomma()}),
    "local_chan_reserve": ch => ({innerHTML: ch.local_chan_reserve.intcomma()}),
    "last_update": ch =>  ({innerHTML: formatDate(ch.last_update).replace(" ago", ""), title: adjustTZ(ch.last_update)}),
    "initiator": ch => ({innerHTML: ch.initiator ? 'Local': 'Remote'}),
  }, {ar_in_target, auto_rebalance})

  let {local_commit, local_chan_reserve, initiator} = inactive_ch_template
  const private_ch_template = Object.assign({}, home_ch_base, {
    "total_sent": ch => ({innerHTML: ch.total_sent.intcomma()}),
    "total_received": ch => ({innerHTML: ch.total_received.intcomma()})
  }, {local_commit, local_chan_reserve, initiator}, {
    "is_active": ch => ({innerHTML: ch.is_active, title: (ch.is_active ? 'Up' : 'Down') + 'time: ' + formatDate(ch.last_update).replace(" ago", ""), style: {backgroundColor: ch.is_active ? green : red} })
  })
  const pending_open_template = {
    "alias": ch => ({innerHTML: `<a href="${window.GRAPH_LINKS}/${window.NETWORK}node/${ch.remote_node_pub}" target="_blank">${ch.alias == '' ? ch.remote_node_pub.substr(0,12) : ch.alias }</a>`, title: ch.remote_node_pub}),
    "channel_point": ch => ({innerHTML: `<a href='${window.NETWORK_LINKS}/${window.NETWORK}tx/${ch.funding_txid}' target="_blank">${ch.channel_point}</a>`}),
    "capacity": ch => ({innerHTML: ch.capacity.intcomma(), title: `Commit Fee: ${ch.commit_fee}`}),
    "local_balance": ch => ({innerHTML: ch.local_balance.intcomma()}),
    "remote_balance": ch => ({innerHTML: ch.remote_balance.intcomma()}),
    "af": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input type="submit" value="${ch.auto_fees ? 'Dis' : 'En'}able">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="8">
      <input type="hidden" name="target" value="0">
    </form>`, style: {backgroundColor: ch.auto_fees? 'rgba(46,160,67,0.15)' : 'rgba(248,81,73,0.15)'} }),
    "fee_rate": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="0" max="100000" name="target" value="${ch.local_fee_rate}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="1">
    </form>`}),
    "base_fee": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="0" max="100000" name="target" value="${ch.local_base_fee}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="0">
    </form>`}),
    "cltv": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="18" max="1000" name="target" value="${ch.local_cltv}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="9">
    </form>`}),
    "amt": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="1" max="100000000" name="target" value="${ch.ar_amt_target}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="2">
    </form>`}),
    "max_cost": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="1" max="100" name="target" value="${ch.ar_max_cost}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="6">
    </form>`}),
    "otarget": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="1" max="100" name="target" value="${ch.ar_out_target}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="4">
    </form>`, style: {backgroundColor: !ch.auto_rebalance ? 'rgba(248,81,73,0.15)' : 'rgba(46,160,67,0.15)'} }),
    "itarget": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input style="text-align:center" id="target" type="number" min="1" max="100" name="target" value="${ch.ar_in_target}">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="3">
    </form>`, style: {backgroundColor: !ch.auto_rebalance ? 'rgba(248,81,73,0.15)' : 'rgba(46,160,67,0.15)'} }),
    "ar": ch => ({innerHTML: `<form action="/update_pending/" method="post">
      <input type="submit" value="${ch.auto_rebalance ? 'Dis': 'En'}able">
      <input type="hidden" name="funding_txid" value="${ch.funding_txid}">
      <input type="hidden" name="output_index" value="${ch.output_index}">
      <input type="hidden" name="update_target" value="5">
      <input type="hidden" name="target" value="0">
    </form>`, style: {backgroundColor: !ch.auto_rebalance ? 'rgba(248,81,73,0.15)' : 'rgba(46,160,67,0.15)'} }),
  }

  let {capacity, local_balance, remote_balance} = pending_open_template
  const pending_closed_template = Object.assign({}, {
    "channel_id" : ch => ({innerHTML: `<a href="/channel?=${ch.chan_id}" target="_blank">${ch.short_chan_id}</a>`, title: ch.channel_point}),
    "peer_alias" : ch => ({innerHTML: `<a href="${window.GRAPH_LINKS}/${window.NETWORK}node/${ch.remote_node_pub}" target="_blank">${ch.alias == '' ? ch.remote_node_pub.substr(0,12) : ch.alias }</a>`, title: ch.remote_node_pub})
  }, {capacity, local_balance, remote_balance },
  {
    "limbo_bal" : ch => ({innerHTML: ch.limbo_balance.intcomma() }),
    "local_commit" : ch => ({innerHTML: ch.local_commit_fee_sat.intcomma() }),
    "closing_tx" : ch => ({innerHTML: `<a href='${window.NETWORK_LINKS}/${window.NETWORK}tx/${ch.closing_txid}' target="_blank">${ch.closing_txid}</a>`}),
  })
  const pending_fclosed_template = Object.assign({}, pending_closed_template)
  pending_fclosed_template["local_commit"] = ch => ({innerHTML: formatDate(ch.maturity_datetime), title: ch.blocks_til_maturity.intcomma()+' blocks ~'+adjustTZ(ch.maturity_datetime) })

  function addTitle(id, results, title = null){
    const container = byId(id.split(" ").join("_").toLowerCase() + '_container')
    if (results.length == 0) {
      container.style.display = 'none'
      return false
    }
    container.style.display = 'block'
    if (container.querySelector('h2')) container.querySelector('h2').innerHTML = null
    container.insertAdjacentHTML('afterbegin', `<h2>${title || id}</h2>`)
    return true
  }
  function build_active(channels, forwards_sum){
    let sum = {inbound: 0, outbound: 0, unsettled: 0, earned:{d1:0, d7:0}, rOUTed:{d1:{amt:0, count:0}, d7:{amt:0, count:0} } }
    if(!addTitle("Active Channels", channels)) return update_liq('active_', sum)

    const [activeChannels, template] = [byId("active_channels"), use(detailed_ch_template)]

    activeChannels.innerHTML = null
    channels.map(ch => { //derived properties
      ch.inbound_can = ch.percent_inbound / ch.ar_in_target
      ch.fee_ratio = ch.local_fee_rate == 0 ? 100 : parseInt(ch.remote_fee_rate*100/ch.local_fee_rate)
      ch.fee_check = parseInt(ch.fee_ratio*100/ch.ar_max_cost)

      sum.inbound += ch.remote_balance
      sum.outbound += ch.local_balance
      sum.unsettled += ch.unsettled_balance

      if(forwards_sum[ch.chan_id]!=undefined){
        ch['routed_in_1day'] = forwards_sum[ch.chan_id].i1dc
        ch['amt_routed_in_1day'] = forwards_sum[ch.chan_id].i1d
        ch['routed_out_1day'] = forwards_sum[ch.chan_id].o1dc
        ch['amt_routed_out_1day'] = forwards_sum[ch.chan_id].o1d
        ch['routed_in_7day'] = forwards_sum[ch.chan_id].i7dc
        ch['amt_routed_in_7day'] = forwards_sum[ch.chan_id].i7d
        ch['routed_out_7day'] = forwards_sum[ch.chan_id].o7dc
        ch['amt_routed_out_7day'] = forwards_sum[ch.chan_id].o7d
      }else{
        ch['routed_in_1day'] = 0
        ch['amt_routed_in_1day'] = 0
        ch['routed_out_1day'] = 0
        ch['amt_routed_out_1day'] = 0
        ch['routed_in_7day'] = 0
        ch['amt_routed_in_7day'] = 0
        ch['routed_out_7day'] = 0
        ch['amt_routed_out_7day'] = 0
      }

      return ch
    }).sort((ch1, ch2) => ch1.percent_outbound - ch2.percent_outbound)
      .forEach(ch => activeChannels.append(template.render(ch)))

    return update_liq('active_', sum)
  }
  function build_private(channels){
    let act = {outbound: 0, inbound: 0}, ina = {outbound: 0, inbound: 0}
    let sum = {earned:{d1:0, d7:0}, rOUTed:{d1:{amt:0, count:0}, d7:{amt:0, count:0} } }
    if(!addTitle("Private Channels", channels)) return Object.assign({}, sum, {outbound: act.outbound + ina.outbound, inbound: act.inbound + ina.inbound})

    let [capacity, active_count, table, template] = [0,0,byId('private_channels'),use(private_ch_template)]
    table.innerHTML = null
    channels.forEach(ch => {
      capacity += ch.capacity
      if(ch.is_active) {
        active_count += 1
        act.outbound += ch.local_balance
        act.inbound += ch.remote_balance
      }
      else {
        ina.outbound += ch.local_balance
        ina.inbound += ch.remote_balance
      }

      table.append(template.render(ch))
    })

    byId("total_channels").innerHTML = (byId("total_channels").innerHTML.toInt() - channels.length)
    byId("public_channels_count").innerHTML = (byId("public_channels_count").innerHTML.toInt() - active_count)
    byId("private_capacity").innerHTML = capacity.intcomma()
    byId("private_liquidity").innerHTML = (act.outbound + ina.outbound).intcomma()
    byId("private_active").innerHTML = active_count
    byId("private_count").innerHTML = channels.length
    byId("private_stats").style = {'display': 'block'}

    return Object.assign({}, sum, {outbound: act.outbound + ina.outbound, inbound: act.inbound + ina.inbound})
  }
  function build_inactive(channels){
    let sum = {inbound: 0, outbound: 0, unsettled: 0}
    if(!addTitle("Inactive Channels", channels)) return update_liq('inactive_', sum)

    const table = byId('inactive_channels'), template = use(inactive_ch_template)
    table.innerHTML = null
    channels.forEach(ch => {
      sum.inbound += ch.remote_balance
      sum.outbound += ch.local_balance
      sum.unsettled += ch.unsettled_balance
      table.append(template.render(ch))
    })

    return update_liq('inactive_', sum)
  }
  function update_liq(status, sum){
    for (id of ['inbound', 'outbound', 'unsettled']){
      byId(status+id).innerHTML = sum[id].intcomma()
    }
    return sum
  }
  async function update_summary(node_info, payments7d, onchain_task, closures_task, active, inactive, private, forwards_summary, inv_rev_task){
    let offChain = {d1: {fee:0, amt:0, inv_rev:0}, d7: {fee:0, amt:0, inv_rev:0} }
    for(p of payments7d){
      if(adjustTZ(p.creation_date) >= _1day_ago){
        offChain.d1.fee += p.fee
        offChain.d1.amt += p.value
      }
      offChain.d7.fee += p.fee
      offChain.d7.amt += p.value
    }
    for(inv of (await inv_rev_task).results){
      if(adjustTZ(inv.creation_date) >= _1day_ago){
        offChain.d1.inv_rev += inv.amt_paid
      }
      offChain.d7.inv_rev += inv.amt_paid
    }
    let chainCosts = {d1: 0, d7:0}
    for(tx of (await onchain_task).results){
      if(adjustTZ(tx.time_stamp) >= _1day_ago){
        chainCosts.d1 += tx.fee
      }
      chainCosts.d7 += tx.fee
    }
    for(close of (await closures_task).results){
      if(close.close_height >= (node_info.block.height - 144)){
        chainCosts.d1 += close.closing_costs
      }
      chainCosts.d7 += close.closing_costs
    }
    function merge(obj, other){
      const result = Object.assign({}, obj)

      for(let [key, value] of Object.entries(other)){
        if(value instanceof Object) result[key] = merge(result[key], value) //deep sum
        else result[key] = (result[key] || 0) + value
      }
      return result
    }

    byId('total_inbound').innerHTML = (active.inbound + inactive.inbound).intcomma()
    byId('total_outbound').innerHTML = (active.outbound + inactive.outbound).intcomma()
    byId('total_unsettled').innerHTML = (active.unsettled + inactive.unsettled).intcomma()

    const public = merge(active, inactive), sum = merge(public, private)
    const [total_outbound, total_bal] = [sum.outbound || 1, byId('total_balance')]
    byId('liq_ratio').innerHTML = (public.inbound*100/public.outbound||1).intcomma()+'%'
    total_bal.innerHTML = (total_bal.innerHTML.toInt() + sum.outbound).intcomma()

    for(d of [1,7]){ //build 1 & 7 days table statistics
      const earned = forwards_summary[`earned${d}d`]+offChain[`d${d}`].inv_rev, routed = {'amt':forwards_summary[`o${d}d`],'count':forwards_summary[`o${d}dc`]}, chaincosts = chainCosts[`d${d}`], offchain = offChain[`d${d}`], costs = chaincosts + offchain.fee
      byId(`routed_${d}day`).innerHTML = `${routed.amt.intcomma()} <small class="w3-round w3-border-small w3-border-grey">${routed.count.intcomma()}</small>`
      byId(`earned_${d}day`).innerHTML = `${earned.intcomma()} <small class="w3-round w3-border-small w3-border-grey">${(earned*1000000/(routed.amt||1)).intcomma()}ₚₚₘ</small>`
      byId(`fees_${d}day`).innerHTML = `${offchain.fee.intcomma()} <small class="w3-round w3-border-small w3-border-grey">${(offchain.fee*1000000/(offchain.amt||1)).intcomma()}ₚₚₘ</small>`
      byId(`onchain_costs_${d}day`).innerHTML = chaincosts.intcomma()
      byId(`percent_cost_${d}day`).innerHTML = (earned ? costs*100/earned : 0).intcomma()+'%'
      byId(`profit_per_outbound_${d}d`).innerHTML = `<span class="w3-round w3-border-small w3-border-grey">⚡${((earned - offchain.fee)*1000000/total_outbound).intcomma()}ₚₚₘ</span>
        <span class="w3-round w3-border-small w3-border-grey">⛓️${((earned - costs)*1000000/total_outbound).intcomma()}ₚₚₘ</span>`
      byId(`routed_${d}day_percent`).innerHTML = parseFloat((routed.amt*100/total_outbound).toFixed(1)).toLocaleString()+'%'
    }
  }
  async function loadForwards(){
    const lastId = forwardsTable.tBodies[0].lastChild.objId
    const forwards_task = GET('forwards', {data: {id__lt: lastId, limit: 10} })
    build_next_routed(forwards_task)
  }
  async function build_next_routed(forwards_task){
    let next_forwards = (await forwards_task).results
    if(next_forwards.length==0){
      byId("loadMoreForwards").style.display = "none"
      return
    }
    const tableBody = forwardsTable.querySelector("tbody")
    for (f of next_forwards){
      f.amt_in = f.amt_in_msat/1000
      f.amt_out = f.amt_out_msat/1000
      f.ppm = f.fee*1000000/f.amt_out
      tableBody.appendChild(use(routed_template).render(f))
    }
  }
  async function build_routed(forwards){
    let template = use(routed_template)

    if(!addTitle('Routed', forwards, `Last <a href="/forwards" target="_blank">Payments Routed</a>`)) return
    const table = byId('routed')
    table.innerHTML = null
    for(f of forwards.slice(0,20)){
      f.amt_in = f.amt_in_msat/1000
      f.amt_out = f.amt_out_msat/1000
      f.ppm = f.fee*1000000/f.amt_out
      table.append(template.render(f))
    }
  }
  async function loadPayments(){
    const lastId = paymentsTable.tBodies[0].lastChild.objId
    const payments_task = GET('payments', {data: {status__lt: 3, index__lt: lastId, limit: 10} })
    let next_payments = (await payments_task).results
    if(next_payments.length==0){
      byId("loadMorePayments").style.display = "none"
      return
    }
    build_next_payments(next_payments)
  }
  async function build_next_payments(next_payments){
    const tableBody = paymentsTable.querySelector("tbody")
    for (p of next_payments){
      p.ppm = p.fee*1000000/p.value
      tableBody.appendChild(use(payments_template).render(p))
    }
  }
  async function build_payments(payments7d){
    let remaining = 10-payments7d.length, template = use(payments_template), payments = payments7d
    if(remaining > 0) payments = [...payments, ...(await GET('payments', {data: {creation_date__lt: _7days_ago, status__lt:3, limit: remaining} })).results]

    if(!addTitle('Payments', payments, `Last 10 <a href="/payments" target="_blank">Payments</a>`)) return
    const table = byId('payments')
    table.innerHTML = null
    for(p of payments.slice(0,10)){
      p.ppm = p.fee*1000000/p.value
      table.append(template.render(p))
    }
  }
  async function loadInvoices(){
    const lastId = invoicesTable.tBodies[0].lastChild.objId
    const invoices_task = GET('invoices', {data: {state__lt: 2, index__lt: lastId, limit: 10} })
    let next_invoices = (await invoices_task).results
    if(next_invoices.length==0){
      byId("loadMoreInvoices").style.display = "none"
      return
    }
    build_next_invoices(next_invoices)
  }
  async function build_next_invoices(next_invoices){
    const tableBody = invoicesTable.querySelector("tbody")
    for (inv of next_invoices){
      tableBody.appendChild(use(invoices_template).render(inv))
    }
  }
  function build_invoices(invoices){
    if(!addTitle('Invoices', invoices, `Last 10 <a href="/invoices" target="_blank">Invoices</a>`)) return
    const table = byId('invoices'), template = use(invoices_template)
    table.innerHTML = null
    for(inv of invoices) {
      table.append(template.render(inv))
    }
  }
  async function loadFailedHTLCs(){
    const lastId = failedhtlcsTable.tBodies[0].lastChild.objId
    const failedhtlcs_task = GET('failedhtlcs', {data: {wire_failure__lt: 99, id__lt: lastId, limit: 10} })
    let next_failed_htlcs = (await failedhtlcs_task).results
    if(next_failed_htlcs.length==0){
      byId("loadMoreFailedHTLCs").style.display = "none"
      return
    }
    build_failedhtlcs(next_failed_htlcs)
  }
  async function build_failedhtlcs(failed_htlcs){
    const tableBody = failedhtlcsTable.querySelector("tbody")
    for (f of failed_htlcs){
      tableBody.appendChild(use(failedHTLCs_template).render(f))
    }
  }
  function build_failedHTLC(fHTLCs){
    if(!addTitle('failed_htlcs', fHTLCs, `Last <a href="/failed_htlcs" target="_blank">Failed HTLCs</a>`)) return
    const table = byId('failed_htlcs'), template = use(failedHTLCs_template)
    table.innerHTML = null
    for(htlc of fHTLCs){
      table.append(template.render(htlc))
    }
  }
  function process_forwards(forwards){
    let forwards_sum = {}
    let forwards_summary = { o1d: 0, o1dc: 0, o7d: 0, o7dc: 0, earned1d: 0, earned7d: 0 }
    for(f of forwards){
      chan_id = f.chan_id

      if(forwards_sum[chan_id] === undefined){ forwards_sum[chan_id]={o1d:0, o1dc:0, o7d:0, o7dc:0, i1d:0, i1dc:0, i7d:0, i7dc:0} }

      forwards_sum[chan_id].o1d += f.sum_outgoing_1day/1000
      forwards_sum[chan_id].o1dc += f.count_outgoing_1day
      forwards_sum[chan_id].i1d += f.sum_incoming_1day/1000
      forwards_sum[chan_id].i1dc += f.count_incoming_1day
      forwards_summary.o1d += f.sum_outgoing_1day/1000
      forwards_summary.o1dc += f.count_outgoing_1day
      forwards_summary.earned1d += f.sum_fees_1day

      forwards_sum[chan_id].o7d += f.sum_outgoing_7day/1000
      forwards_sum[chan_id].o7dc += f.count_outgoing_7day
      forwards_sum[chan_id].i7d += f.sum_incoming_7day/1000
      forwards_sum[chan_id].i7dc += f.count_incoming_7day
      forwards_summary.o7d += f.sum_outgoing_7day/1000
      forwards_summary.o7dc += f.count_outgoing_7day
      forwards_summary.earned7d += f.sum_fees_7day
    }
    return [forwards_sum, forwards_summary]
  }

  function buildMainStats(node_info) {
    const lnd = byId('lnd')
    lnd.classList.remove('w3-red')
    lnd.classList.remove('w3-green')
    lnd.classList.toggle('w3-red', !node_info.synced_to_graph)
    lnd.classList.toggle('w3-green', node_info.synced_to_graph)

    const timechain = byId('timechain')
    timechain.classList.remove('w3-border-red')
    timechain.classList.remove('w3-border-green')
    timechain.classList.toggle('w3-border-red', !node_info.synced_to_chain)
    timechain.classList.toggle('w3-border-green', node_info.synced_to_chain)
    timechain.title = `Chain ${!node_info.synced_to_chain ? 'Not' : ''} Synced`
    timechain.innerHTML = node_info.chains.join(', ') + ` | ${node_info.block.height} #${node_info.block.hash}`

    byId('public_channels_count').innerHTML = node_info.num_active_channels
    byId('total_channels').innerHTML = node_info.num_active_channels + node_info.num_inactive_channels
    byId('peers').innerHTML = `<a href="/peers" target="_blank">Peers</a>: ${node_info.num_peers}`
    if(node_info.db_size > 0) byId('dbSize').innerHTML = `DB Size: ${node_info.db_size} GB`

    byId('total_balance').innerHTML = node_info.balance.total.intcomma()
    byId('onChain').innerHTML = `Onchain: ${node_info.balance.onchain.intcomma()}`
    byId('confirmed').innerHTML = `Confirmed: ${node_info.balance.confirmed.intcomma()}`
    byId('unconfirmed').innerHTML = `Unconfirmed: ${node_info.balance.unconfirmed.intcomma()}`
    byId('limbo').innerHTML = `Limbo: ${node_info.balance.limbo.intcomma()}`
  }
  function build(id, template_arg, channels){
    if(!addTitle(id, channels)){
      return;
    }
    const table = byId(id.split(" ").join("_").toLowerCase()), template = use(template_arg)
    table.innerHTML = null
    channels.forEach(ch => table.append(template.render(ch)))
  }
  async function buildMain() {
    const node_info = await GET('node_info', {data: {limit:1}})
    buildMainStats(node_info)
    const inv_rev_task = GET('invoices', {data: {state__lt: 2, is_revenue: true, settle_date__gte: _7days_ago}})
    const onchain_task = GET('onchain', {data: {time_stamp__gte: _7days_ago} })
    const forwardsSummary_task = GET('forwards_summary', {data: {} })
    const forwardsList_task = GET('forwards', {data: {limit: 20} })
    const payments_task = GET('payments', {data: {creation_date__gte: _7days_ago, status__lt: 3} })
    const closures_task = GET('closures', {data: {close_height__gte: (node_info.block.height - 1008) } })
    const invoices_task = GET('invoices', {data: {state__lt: 2, limit: 10} })
    const failedHTLCs_task = GET('failedhtlcs', {data: {wire_failure__lt: 99, limit:10} })

    let [updates, pending_htlcs, active, inactive, private, public_capacity] = [0,0,[],[],[],0]
    for (ch of (await GET('channels', {data: {is_open:true} })).results){ //basic setup
      updates += ch.num_updates
      pending_htlcs += ch.htlc_count
      ch.local_balance += ch.pending_outbound
      ch.remote_balance += ch.pending_inbound
      ch.percent_inbound = parseInt(ch.remote_balance*100/ch.capacity)
      ch.percent_outbound = parseInt(ch.local_balance*100/ch.capacity)

      if(ch.private) private.push(ch)
      else {
        (ch.is_active ? active : inactive).push(ch)
        public_capacity += ch.capacity
      }
    }

    byId("updates").innerHTML = updates.intcomma()
    byId("pending_htlcs").innerHTML = pending_htlcs.intcomma()
    byId('public_capacity').innerHTML = public_capacity.intcomma()

    let [forwardsSummary, payments7d] = [(await forwardsSummary_task).results, (await payments_task).results]
    let [forwards_sum, forwards_summary] = process_forwards(forwardsSummary)
    let [activeSum, privateSum, inactiveSum] = [build_active(active, forwards_sum), build_private(private), build_inactive(inactive)]
    update_summary(node_info, payments7d, onchain_task, closures_task, activeSum, inactiveSum, privateSum, forwards_summary, inv_rev_task)
    build('Pending Open', pending_open_template, node_info.pending_open||[])
    build('Waiting For Close', pending_closed_template, node_info.waiting_for_close||[])
    build('Pending Force Closed', pending_fclosed_template, node_info.pending_force_closed||[])
    build_routed((await forwardsList_task).results)
    build_payments(payments7d)
    build_invoices((await invoices_task).results)
    build_failedHTLC((await failedHTLCs_task).results)
    byId("datetime").innerHTML = new Date().toLocaleTimeString();
    await auto_refresh(buildMain)
  }
  document.addEventListener('DOMContentLoaded', async () => {
    const builder = buildMain()
    await builder
  })
