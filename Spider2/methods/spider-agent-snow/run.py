import argparse
import datetime
import json
import logging
import os
import random
import sys
import glob
from pathlib import Path

from tqdm import tqdm

from spider_agent.envs.spider_agent import Spider_Agent_Env
from spider_agent.agent.agents import PromptAgent


#  Logger Configs {{{ #
logger = logging.getLogger("spider_agent")
logger.setLevel(logging.DEBUG)

datetime_str: str = datetime.datetime.now().strftime("%Y%m%d@%H%M%S")

file_handler = logging.FileHandler(os.path.join("logs", "normal-{:}.log".format(datetime_str)), encoding="utf-8")
debug_handler = logging.FileHandler(os.path.join("logs", "debug-{:}.log".format(datetime_str)), encoding="utf-8")
stdout_handler = logging.StreamHandler(sys.stdout)
sdebug_handler = logging.FileHandler(os.path.join("logs", "sdebug-{:}.log".format(datetime_str)), encoding="utf-8")

file_handler.setLevel(logging.INFO)
debug_handler.setLevel(logging.DEBUG)
stdout_handler.setLevel(logging.INFO)
sdebug_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    fmt="\x1b[1;33m[%(asctime)s \x1b[31m%(levelname)s \x1b[32m%(module)s/%(lineno)d-%(processName)s\x1b[1;33m] \x1b[0m%(message)s")
file_handler.setFormatter(formatter)
debug_handler.setFormatter(formatter)
stdout_handler.setFormatter(formatter)
sdebug_handler.setFormatter(formatter)

stdout_handler.addFilter(logging.Filter("spider_agent"))
sdebug_handler.addFilter(logging.Filter("spider_agent"))

logger.addHandler(file_handler)
logger.addHandler(debug_handler)
logger.addHandler(stdout_handler)
logger.addHandler(sdebug_handler)
#  }}} Logger Configs # 



def config() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run end-to-end evaluation on the benchmark"
    )
    
    parser.add_argument("--max_steps", type=int, default=20)
    
    parser.add_argument("--max_memory_length", type=int, default=30)
    parser.add_argument("--suffix", '-s', type=str, default="gpt-4-try1")
    
    parser.add_argument("--model", type=str, default="gpt-4o")
    parser.add_argument("--temperature", type=float, default=0.5)
    parser.add_argument("--top_p", type=float, default=0.9)
    parser.add_argument("--max_tokens", type=int, default=2500)
    parser.add_argument("--stop_token", type=str, default=None)
    
    # example config
    parser.add_argument("--test_path","-t", type=str, default="./examples/spider2-snow.jsonl")
    parser.add_argument("--example_index", "-i", type=str, default="all", help="index range of the examples to run, e.g., '0-10', '2,3', 'all'")
    parser.add_argument("--example_name", "-n", type=str, default="", help="name of the example to run")
    parser.add_argument("--N", type=int, default=None, help="Run only the first N testcases after selection/filtering.")
    parser.add_argument("--overwriting", action="store_true", default=False)
    parser.add_argument("--retry_failed", action="store_true", default=False)

    # output related
    parser.add_argument("--output_dir", type=str, default="output")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--bq_only", action="store_true")
    parser.add_argument("--local_only", action="store_true")
    parser.add_argument("--dbt_only", action="store_true")
    parser.add_argument("--sf_only", action="store_true")
    
    
    args = parser.parse_args()

    return args



def test(
    args: argparse.Namespace,
    test_all_meta: dict = None
) -> None:
    scores = []
    
    # log args
    logger.info("Args: %s", args)

    if args.suffix == "":
        logger.warning("No suffix is provided, the experiment id will be the model name.")
        experiment_id = args.model.split("/")[-1]
    else:
        experiment_id = args.model.split("/")[-1] + "-" + args.suffix
        
    if args.plan:
        experiment_id = f"{experiment_id}-plan"

    env_config = \
    {
        "image_name": "spider_agent-image",
        "init_args": {
            "name": experiment_id,
            "work_dir": "/workspace",
        }
    }
    
    agent = PromptAgent(
        model=args.model,
        max_tokens=args.max_tokens,
        top_p=args.top_p,
        temperature=args.temperature,
        max_memory_length=args.max_memory_length,
        max_steps=args.max_steps,
        use_plan=args.plan
    )
    valid_ids = []
    attempted_count = 0
    finished_count = 0
    failed_count = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_llm_calls = 0
    ## load task configs
    assert os.path.exists(args.test_path) and args.test_path.endswith(".jsonl"), f"Invalid test_path, must be a valid jsonl file: {args.test_path}"
    with open(args.test_path, "r") as f:
        task_configs = [json.loads(line) for line in f]

        
    if args.example_name != "":
        task_configs = [task for task in task_configs if args.example_name in task["id"]]
    else:
        if args.example_index != "all":
            if "-" in args.example_index:
                start, end = map(int, args.example_index.split("-"))
                task_configs = task_configs[start:end]
            else:
                indices = list(map(int, args.example_index.split(",")))
                task_configs = [task_configs[i] for i in indices]

    if args.N is not None:
        if args.N <= 0:
            raise ValueError("--N must be a positive integer when provided.")
        original_total = len(task_configs)
        task_configs = task_configs[:args.N]
        logger.info("Limiting to first %d testcase(s) via --N (from %d selected).", len(task_configs), original_total)
    
    for task_config in task_configs:
        instance_id = experiment_id +"/"+ task_config["instance_id"]
        output_dir = os.path.join(args.output_dir, instance_id)
        result_json_path =os.path.join(output_dir, "spider/result.json")


        task_type = None
        if task_config["instance_id"].startswith("bq") or task_config["instance_id"].startswith("ga"):
            task_type = 'bq'
        elif task_config["instance_id"].startswith("local"):
            task_type = 'local'
        elif task_config["instance_id"].startswith("sf"):
            task_type = 'sf'
        else:
            task_type = 'dbt'

        valid_types = set()
        if args.local_only: valid_types.add('local')
        if args.bq_only: valid_types.add('bq')
        if args.sf_only: valid_types.add('sf')
        if args.dbt_only: valid_types.add('dbt')
        
        if  (args.local_only or args.bq_only or args.sf_only or args.dbt_only):
            if task_type not in valid_types: continue
        else:
            pass

        valid_ids.append(task_config["instance_id"])
        
        if not args.overwriting and os.path.exists(result_json_path):
            logger.info("Skipping %s", instance_id)
            continue
        elif os.path.exists(result_json_path):
            logger.info("Overwriting %s", instance_id)
        else:
            logger.info("Running %s", instance_id)
        if args.retry_failed and os.path.exists(result_json_path):
            with open(result_json_path, "r") as f:
                result = json.load(f)
                if result["finished"] and (not "FAIL" in result["result"]) and (not "error" in result["result"].lower()):
                    logger.info("Skipping %s", instance_id)
                    continue
            logger.info("Retrying %s", instance_id)

        if os.path.exists(output_dir):
            os.system(f"rm -rf {output_dir}")
            logger.info("Removed existing %s", output_dir)

        os.makedirs(output_dir, exist_ok=True)

        env_config["init_args"]["name"] = experiment_id +"-"+ task_config["instance_id"]

        
        source_data_dir = os.path.dirname(args.test_path)        
        task_config['config'] = [{"type": "copy_all_subfiles", "parameters": {"dirs": [os.path.join(source_data_dir, task_config["instance_id"])]}}]

        env = Spider_Agent_Env(
            env_config=env_config,
            task_config=task_config,
            cache_dir="./cache",
            mnt_dir=output_dir
        )
    
        agent.set_env_and_task(env)
    
        logger.info('Task input:' + task_config['instruction'])
        done, result_output = agent.run()
        trajectory = agent.get_trajectory()
        attempted_count += 1

        os.makedirs(os.path.join(output_dir, "spider"), exist_ok=True)
        result_files = env.post_process()
        spider_result = {"finished": done, "steps": len(trajectory["trajectory"]),
                           "result": result_output,"result_files": result_files, **trajectory}
        with open(os.path.join(output_dir, "spider/result.json"), "w") as f:
            json.dump(spider_result, f, indent=2)

        token_usage = trajectory.get("token_usage", {})
        total_prompt_tokens += int(token_usage.get("prompt_tokens", 0) or 0)
        total_completion_tokens += int(token_usage.get("completion_tokens", 0) or 0)
        total_tokens += int(token_usage.get("total_tokens", 0) or 0)
        total_llm_calls += int(token_usage.get("llm_calls", 0) or 0)

        failed_output = isinstance(result_output, str) and (
            "FAIL" in result_output or "error" in result_output.lower()
        )
        if done and not failed_output:
            finished_count += 1
        else:
            failed_count += 1
            
            
        
        # Delete sqlite files
        if task_type == 'local':
            sqlite_files = glob.glob(os.path.join(output_dir, '*.sqlite')) + glob.glob(os.path.join(output_dir, '*.duckdb'))

            for file_path in sqlite_files:
                try:
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
        
        
        logger.info("Finished %s", instance_id)
        env.close()

    run_accuracy = (finished_count / attempted_count) if attempted_count else 0.0
    logger.info(
        "Run summary | attempted=%d finished=%d failed=%d accuracy=%.4f",
        attempted_count,
        finished_count,
        failed_count,
        run_accuracy,
    )
    logger.info(
        "Token summary | calls=%d prompt_tokens=%d completion_tokens=%d total_tokens=%d",
        total_llm_calls,
        total_prompt_tokens,
        total_completion_tokens,
        total_tokens,
    )

    summary_dir = Path(args.output_dir) / experiment_id
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_path = summary_dir / "token_usage_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "attempted_cases": attempted_count,
                "finished_cases": finished_count,
                "failed_cases": failed_count,
                "accuracy": run_accuracy,
                "llm_calls": total_llm_calls,
                "prompt_tokens": total_prompt_tokens,
                "completion_tokens": total_completion_tokens,
                "total_tokens": total_tokens,
            },
            f,
            indent=2,
        )
    logger.info("Saved token usage summary to %s", summary_path)

if __name__ == '__main__':
    args = config()
    
    test(args)