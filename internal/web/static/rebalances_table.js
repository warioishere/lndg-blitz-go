// Ausgelagerter Inline-Script aus gui/templates/rebalances_table.html.
// Go html/template kann JS-Template-Literals (`${...}`) nicht inline escapen,
// daher liegt die Logik hier statisch und wird pro Tabelle mit den Server-
// Parametern aufgerufen (status/count/load_count/payment_hash/last_hop_pubkey).
function initRebalancesTable(params) {
  function formatDuration(duration){
    return `${duration} minute${duration === 1 ? 's':''}`
  }
  const statuses = {0: 'Pending', 1: 'In-Flight', 2: 'Successful', 3: 'Timeout', 4: 'No-Route', 5: 'Error', 6: 'Incorrect-Payment-Details', 7: 'Insufficient-Balance', 400: 'Rebalancer-Request-Failed', 406: 'No-Sources', 408: 'Rebalancer-Request-Timeout', 499: 'Cancelled'}
  const transformations = {
    'requested': rebalance => ({innerHTML: formatDate(rebalance.requested), title: adjustTZ(rebalance.requested)}),
    'start': rebalance => ({innerHTML: formatDate(rebalance.start), title: rebalance.start ? adjustTZ(rebalance.start) : '---'}),
    'stop': rebalance => ({innerHTML: formatDate(rebalance.stop), title: rebalance.stop ? adjustTZ(rebalance.stop) : '---'}),
    'duration': rebalance => ({innerHTML: formatDuration(rebalance.duration)}),
    'elapsed': rebalance => ({innerHTML: formatDate(rebalance.start, rebalance.stop).replace(" ago", "")}),
    'value': rebalance => ({innerHTML: Number(rebalance.value).toLocaleString()}),
    'fee_limit': rebalance => ({innerHTML: Number(rebalance.fee_limit).toLocaleString()}),
    'ppm': rebalance => ({innerHTML: Math.round(rebalance.fee_limit*1000000/rebalance.value).toLocaleString()}),
    'fees_paid': rebalance => ({innerHTML: rebalance.status == 2 ? Number(rebalance.fees_paid).toLocaleString() : '---'}),
    'target_alias': rebalance => ({innerHTML: rebalance.target_alias !== '' ? rebalance.target_alias : '---'}),
    'status': rebalance => ({innerHTML: statuses[rebalance.status], title: rebalance.status}),
    'payment_hash': rebalance => ({innerHTML: (rebalance.payment_hash || '').length == 0 ? '---' :
         `<a href="/route?=${rebalance.payment_hash}" target="_blank">${rebalance.payment_hash.substring(0,7)}</a>`}),
    'action': rebalance => {
      const input = document.createElement("button")
      if (rebalance.status != 0){
        input.innerHTML = "🔂"
        input.title="Repeat this request"
        input.onclick = async function(){
          const {value, fee_limit, outgoing_chan_ids, last_hop_pubkey, duration, target_alias, manual} = rebalance
          if (last_hop_pubkey.length === 0){
            var new_rebal = {value, fee_limit, outgoing_chan_ids, duration, manual}
          }else{
            var new_rebal = {value, fee_limit, outgoing_chan_ids, last_hop_pubkey, duration, target_alias, manual}
          }
          const response = await POST('rebalancer', {body: new_rebal})
          const table = input.parentElement.parentElement.parentElement
          table.prepend(use(transformations).render(response))
        }
      }
      else {
        input.innerHTML = "❌"
        input.title="Cancel this request"
        input.onclick = async function(){
          const response = await PUT(`rebalancer/${rebalance.id}`, {body: {status: 499}})

          const table = input.parentElement.parentElement.parentElement
          const index = input.parentElement.parentElement.rowIndex -1
          table.deleteRow(index)
          use(transformations).render(response, table.insertRow(index))
        }
      }
      return {innerHTML: input}
    }
  };
  async function buildTable(){
    const res = await GET('rebalancer', {data: {limit: params.count, status: params.status, payment_hash: params.payment_hash, last_hop_pubkey: params.last_hop_pubkey }})
    if(res.errors) return
    if(res.results.length == 0){
      document.getElementById("rebalances_div_"+params.status).innerHTML = `<center><h1>You dont have any ${params.status=='' ? '' : statuses[params.status].toLowerCase()} rebalance request</h1></center>`
      return
    }

    const table = document.getElementById("rebalances_"+params.status), template = use(transformations)
    table.innerHTML = null
    res.results.forEach(rebalance => table.append(template.render(rebalance)));

    await auto_refresh(buildTable)
  }

  document.addEventListener('DOMContentLoaded', buildTable);

  window["loadRebals_"+params.status] = async function(){
    const status = params.status
    const rebalsTable = byId('rebalsTable_'+status)
    const lastId = rebalsTable.tBodies[1].lastChild.objId
    const rebals_task = GET('rebalancer', {data: {last_hop_pubkey: params.last_hop_pubkey, id__lt: lastId, status: status, limit: params.load_count} })
    build_rebals(rebals_task, rebalsTable, status)
  }
  async function build_rebals(rebals_task, rebalsTable, status){
    let next_rebals = (await rebals_task).results
    if(next_rebals.length==0){
      byId("loadMoreRebals_"+status).style.display = "none"
      return
    }
    const tableBody = rebalsTable.tBodies[1]
    next_rebals.forEach(rebalance => tableBody.append(use(transformations).render(rebalance)))
  }
}
