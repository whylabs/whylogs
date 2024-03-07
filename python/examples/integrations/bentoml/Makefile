.PHONY: start-thread start-process request-thread-model-iris request-thread-model-custom request-process-model-iris request-process-model-custom help default

default: help

start-thread: ## Start the server with thread logger
	@: $${WHYLABS_API_KEY:?Error: WHYLABS_API_KEY env var is not set}
	poetry run bentoml serve server_thread_logger:svc

start-process: ## Start the server with process logger
	@: $${WHYLABS_API_KEY:?Error: WHYLABS_API_KEY env var is not set}
	poetry run bentoml serve server_process_logger:svc

request-thread-model-iris: ## Make a request to the iris model with thread logger based server
	curl --header "Content-Type: application/json" \
		--request POST \
		--data '[[4.9, 3.0, 1.4, 0.2]]' \
		http://localhost:3000/v1/models/iris/predict

request-thread-model-custom: ## Make a request to the other model with thread logger based server
	curl --header "Content-Type: application/json" \
		--request POST \
		--data '{"C": [1, 2], "D": [3, 4]}' \
		http://localhost:3000/v1/models/custom_model/predict

request-process-model-iris: ## Make a request to the iris model with process logger based server
	curl --header "Content-Type: application/json" \
		--request POST \
		--data '[[4.9, 3.0, 1.4, 0.2]]' \
		http://localhost:3000/v1/models/iris/predict

request-process-model-custom: ## Make a request to the other model with process logger based server
	curl --header "Content-Type: application/json" \
		--request POST \
		--data '{"C": [1, 2], "D": [3, 4]}' \
		http://localhost:3000/v1/models/custom_model/predict

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
