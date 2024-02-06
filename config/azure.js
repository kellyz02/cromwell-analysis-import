const azureConfig = {
  cromwellAccountKey: process.env.CROMWELL_STORAGE_ACCOUNT_KEY,
  workflowsContainer: process.env.CROMWELL_STORAGE_ACCOUNT_CONTAINER,
  executionsContainer: process.env.CROMWELL_STORAGE_ACCOUNT_EXECUTIONS, 
  inputsContainer: process.env.CROMWELL_STORAGE_ACCOUNT_INPUTS,
  workflowTemplatesContainer: process.env.CROMWELL_STORAGE_ACCOUNT_WORKFLOW_TEMPLATES, 
  outputsContainer: process.env.CROMWELL_STORAGE_ACCOUNT_OUTPUTS, 
  workflowLogsContainer: process.env.CROMWELL_STORAGE_ACCOUNT_WORKFLOW_LOGS, 
  cromwellAccount: process.env.CROMWELL_STORAGE_ACCOUNT, 
  storedPolicyName: process.env.STORED_POLICY_NAME, 
  atlasAccount: process.env.ATLAS_STORAGE_ACCOUNT, 
  atlasStagingContainer: process.env.ATLAS_STAGING_STORAGE_CONTAINER, 
  atlasDropboxContainer: process.env.ATLAS_DROPBOX_STORAGE_CONTAINER, 
  atlasAccountKey: process.env.ATLAS_STORAGE_ACCOUNT_KEY,
  cromwellOutputsAccount: process.env.OUTPUT_STORAGE_ACCOUNT, 
  cromwellOutputsAccountKey: process.env.OUTPUT_STORAGE_KEY,
  stagingFileDirectory: process.env.STAGING_FILE_DIRECTORY
};

module.exports = { azureConfig };