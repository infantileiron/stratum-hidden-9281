from fastapi import HTTPException
from src.constants.enums import ReviewStatus
from src.utils import repository as repo
from src.utils.identity import new_id, now
def list_reviews(): return repo.all_rows("reviews")
def create(data,user):
    run_id=data["run_id"] if isinstance(data,dict) else data.run_id
    comment=data["comment"] if isinstance(data,dict) else data.comment
    # Serialise with run retry/recovery on the same run row: a review is only
    # created against the currently completed result, and once created the run
    # can no longer be retried.
    with repo.transaction() as (conn,q):
        run=repo.read(conn,"runs",run_id,q,lock=True)
        if run["status"]!="completed": raise ValueError("只有已完成的运行可提交复核")
        reviews=repo.rows_in(conn,"reviews",q)
        if any(r["run_id"]==run_id and r["status"] in ("pending","approved") for r in reviews): raise HTTPException(409,"该运行已有待审或通过的复核")
        item={"id":new_id("rev"),"run_id":run_id,"comment":comment,"status":"pending","reviewer":"","created_at":now()}
        repo.upsert(conn,"reviews",item,q)
        return item
def decide(id,data,user):
    status=data["status"] if isinstance(data,dict) else data.status
    comment=data["comment"] if isinstance(data,dict) else data.comment
    with repo.transaction() as (conn,q):
        item=repo.read(conn,"reviews",id,q,lock=True)
        if item["status"]!=ReviewStatus.PENDING.value: raise HTTPException(409,"该复核已处理")
        item.update(status=status,comment=comment,reviewer=user["email"])
        repo.upsert(conn,"reviews",item,q)
        return item
