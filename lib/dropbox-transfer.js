const { azureConfig } = require("../config/azure");
const { atlasConfig } = require('../config/atlas');

const { cromwellAccountKey, executionsContainer, inputsContainer, workflowTemplatesContainer, outputsContainer, workflowLogsContainer, workflowsContainer, cromwellAccount, atlasAccount, atlasStagingContainer, atlasAccountKey, atlasDropboxContainer, storedPolicyName, cromwellOutputsAccount, cromwellOutputsAccountKey} = azureConfig;
const path = require("path"); 
const deleteOptions = { deleteSnapshots: "include"}; 

// Checks the copy status of all copying files associated with the workflow
// Returns set of source URLs (source files from Cromwell)
const checkCopyStatus = async (stageUrlArray, context) => {
    const { getBlobProps, createBlobClient, getBlobSasUri, listBlobsFlat } = await import('@simalicrum/azure-helpers'); 
    let sourceUrlArray = []; 

    for (let copyingFile of stageUrlArray) {
        const props = await getBlobProps(copyingFile, atlasAccountKey); 
        if (props.copyStatus === "success") {
            sourceUrlArray.push(props.copySource); 
        } else {
            context.log(copyingFile + " is still copying..."); 
        }
    }

    if (sourceUrlArray.length === stageUrlArray.length) {
        return [...new Set(sourceUrlArray)]; // return array of only unique values
    } else {
        return false; 
    }
}

// Copies files from the Atlas cromwellimports "Staging" container to the Atlas Dropbox
const stagingToDropbox = async (stageUrlArray, context) => {
    const { createBlobClient, getBlobSasUri, createUrl} = await import('@simalicrum/azure-helpers');
    for (let stageUrl of stageUrlArray) {
        // to get path from url relative to AXID directory + correctly format for request
        let containerPath = stageUrl.substring(stageUrl.indexOf('/', 1)).substring(stageUrl.indexOf('/') + 1); 
        containerPath = containerPath.substring(containerPath.indexOf("/") + 1); 
        containerPath = containerPath.substring(containerPath.indexOf("/") + 1); 
        containerPath = containerPath.replace(/%2F/g, "/");  

        let dropboxUrl = createUrl(containerPath, atlasAccount, atlasDropboxContainer);
        const dropboxBlobClient = createBlobClient(dropboxUrl, atlasAccountKey);   
        const stageSasUri = getBlobSasUri(containerPath, atlasStagingContainer, atlasAccount, atlasAccountKey, storedPolicyName); 

        const copied = await dropboxBlobClient.beginCopyFromURL(stageSasUri); 
        const operationState = copied.getOperationState(); 
        const properties = await operationState.blobClient.getProperties(); 
        if (properties._response.status === 200 && (properties.copyStatus === "pending" || properties.copyStatus === "success")) {     
            const axId = containerPath.split("/", 2)[0]; 
            const fileName = path.basename(containerPath);
            context.log("COPYING to Dropbox (" + axId + "): " + fileName); 
        } else {
            throw new Error('Copy process of ' + fileName + ' to output storage failed. Aborting cleanup!'); 
        }
    }
}


// Deletes source files in Cromwell storage account
const deleteSourceFiles = async (sourceUrlArray, context) => {
    const { deleteBlob, createBlobClient } = await import('@simalicrum/azure-helpers'); 
    
    for (let sourceUrl of sourceUrlArray) {
        let containerPath = sourceUrl.substring(sourceUrl.indexOf('/', 1)).substring(sourceUrl.indexOf('/') + 1); 
        const pathEnding = path.extname(containerPath); 
        const fileExt = pathEnding.split("?")[0]; 

        if (fileExt !== ".wdl") {
            const blobClient = createBlobClient(sourceUrl, cromwellAccountKey); 
            const deleteSource = await blobClient.deleteIfExists(deleteOptions); 
            context.log("DELETED: " + sourceUrl); 
        }
    }
}

// Deletes the entire executions directory for the workflow
const deleteExecutions = async (workflowId, workflowType, context) => {
    const prefix = workflowType + "/" + workflowId; 
    const { deleteBlob, createUrl, listBlobsFlat } = await import('@simalicrum/azure-helpers'); 
    let deleteCount = 0; 
    for await (const blob of listBlobsFlat(cromwellAccount, cromwellAccountKey, executionsContainer, { prefix: prefix })) {
        const url = createUrl(blob.name, cromwellAccount, executionsContainer); 
        const deleted = await deleteBlob(url, cromwellAccountKey, deleteOptions); 
        deleteCount++; 
    }
    context.log("Workflow executions directory completely deleted! " + deleteCount + " files deleted."); 
}

// Deletes files in the Atlas cromwellimport "Staging" container
const deleteStagingFiles = async (stageUrlArray, context) => {
    const { deleteBlob, createUrl, listBlobsFlat, createBlobClient } = await import('@simalicrum/azure-helpers'); 
    for (let stageUrl of stageUrlArray) {
        const blobClient = createBlobClient(stageUrl, atlasAccountKey); 
        const deleteStage = await blobClient.deleteIfExists(deleteOptions); 
        context.log("DELETED: " + stageUrl); 
    }
}

module.exports = { checkCopyStatus, deleteSourceFiles, stagingToDropbox, deleteExecutions, deleteStagingFiles };
