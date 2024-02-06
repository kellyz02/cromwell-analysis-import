const { azureConfig } = require("../config/azure")
const { cromwellAccountKey, workflowsContainer, cromwellAccount, atlasAccount, atlasStagingContainer, atlasAccountKey, stagingFileDirectory } = azureConfig;
const { checkCopyStatus, deleteSourceFiles, stagingToDropbox, deleteExecutions, deleteStagingFiles } = require("../lib/dropbox-transfer"); 
const path = require("path"); 


module.exports = async function (context, myTimer) {
    const {createUrl, readBlob, deleteBlob, listBlobsFlat, createBlobClient } = await import("@simalicrum/azure-helpers"); 

    var timeStamp = new Date().toISOString();
    
    if (myTimer.isPastDue)
    {
        context.log('JavaScript is running late!');
    }

    const txtFileRegex = new RegExp(".*\.txt")
    let inProgressCount = 0; 
    let completedAndTransferCount = 0; 
    
    try {
        context.log("atlas account: " + atlasAccount); 
        context.log("account key: " + atlasAccountKey); 
        for await (const blob of listBlobsFlat(String(atlasAccount), String(atlasAccountKey), String(atlasStagingContainer), { prefix: String(stagingFileDirectory) })) {
            if (blob.name.match(txtFileRegex)) {
                const txtName = path.basename(blob.name, '.txt'); 
                const nameParts = txtName.split("."); 
                const workflowType = nameParts[0]; 
                const workflowId = nameParts[1]; 
                const txtUrl = createUrl(blob.name, atlasAccount, atlasStagingContainer); 
                const txtContents = await readBlob(txtUrl, atlasAccountKey); 
                const stageUrlArray = txtContents.split(","); 
                const sourceUrlArray = await checkCopyStatus(stageUrlArray, context); 

                // if all files finish copying, array will be returned; otherwise false + will be checked again next trigger
                if (sourceUrlArray) { 
                    const toDropbox = await stagingToDropbox(stageUrlArray, context); 
                    const cleanSourceFiles = await deleteSourceFiles(sourceUrlArray, context); 
                    const cleanExecutions = await deleteExecutions(workflowId, workflowType, context); 
                    const cleanStagingFiles = await deleteStagingFiles(stageUrlArray, context); 
                    const deleteTxtFile = await deleteBlob(txtUrl, atlasAccountKey, { deleteSnapshots: "include"}); 
                    completedAndTransferCount++; 
                    context.log("Workflow " + workflowId + " clean-up complete!"); 
                } else {
                    inProgressCount++;
                    context.log("Workflow " + workflowId + " copying still in progress...")
                }
            }
        }
    } catch(err) {
        context.log(err); 
        throw(err); 
    }

    
    context.log("Scheduled check ran at [" + timeStamp + "]: \nCopy in progress workflows: " + inProgressCount + "\nCompleted + Transferred Workflows: " + completedAndTransferCount);   
};