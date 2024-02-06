const { azureConfig } = require("../config/azure");
const { atlasConfig } = require('../config/atlas');
const { labGroup } = atlasConfig;
const { getWgsAnalysisByUid } = require("./atlas-helpers"); 
const path = require("path");
const axios = require('axios');  


const { format, startOfWeek, endOfWeek } = require("date-fns"); 

const { cromwellAccountKey, executionsContainer, inputsContainer, workflowTemplatesContainer, outputsContainer, workflowLogsContainer, workflowsContainer, cromwellAccount, atlasAccount, atlasStagingContainer, atlasAccountKey, storedPolicyName, cromwellOutputsAccount, cromwellOutputsAccountKey } = azureConfig;
const wgs = "wholegenomesequencing"; 

// Obtain dated folder name for archives using date-fns library 
const getFolderDate = async (inputsUrl) => {
    const { createBlobClient } = await import("@simalicrum/azure-helpers"); 

    // Date is last modified date of inputs.json 
    const inputsBlobClient = createBlobClient(inputsUrl, cromwellAccountKey); 
    const properties = await inputsBlobClient.getProperties(); 
    let uploadDate = properties.lastModified; 

    // Range: Sunday -> Saturday 
    const start = startOfWeek(uploadDate); 
    const end = endOfWeek(uploadDate); 
    const startString = format(start, "MMM'_'d"); 
    const endString = format(end, "MMM'_'d'_'yy"); 

    const directoryName = startString + "-" + endString; 
    return directoryName.toLowerCase(); 
}

// Use analysis to find sample and get patient ID from sample
const getPatientId = async(axId, token, atlasApiUrl, context) => {
    const { getSample, getExperiment } = await import('atlas-helpers'); 

    const xpId = await findXpIterative(axId, token, atlasApiUrl, context); 
    const experiment = await getExperiment(xpId, token, atlasApiUrl, labGroup); 
    const sampleId = experiment.data.data.fk_samples[0].s_sample_uid; 
    const sample = await getSample(sampleId, token, atlasApiUrl, labGroup);
    const patientId = sample[0].name; 
    context.log("Patient ID: " + patientId); 
    return patientId; 
}

// instead of using the program's stack, use a "stack" made up of an array 
const findXpIterative = async(axId, token, atlasApiUrl, context) => {
    let stack = []; 
    stack.push(axId); 

    while (stack.length) {
        let currAx = stack.pop(); 
        const getAnalyses = await getWgsAnalysisByUid(currAx, token, atlasApiUrl, labGroup); 
        const analyses = getAnalyses.data.data; 
        let analysis; 

        // grab the actual AX object that matches the popped id (getter returns all related AX's too)
        for (let a of analyses) {
            if (a.fk_ax_uid === currAx) {
                analysis = a; 
            }
        }

        if (analysis.fk_experiments.length === 1) {
            context.log("Related Experiment: XP" + analysis.fk_experiments[0]); 
            return analysis.fk_experiments[0];   
        } else {
            const relatedAnalyses = analysis.fk_analyses; 
            stack.push(...relatedAnalyses); 
        }
    }
}

// directory must be one of: source, documentation, results 
const copyToAtlasStaging = async (fileUrl, axId, directory, sourceContainer, sourceContainerStoredPolicy, context, fileUrlArray) => {
    const { createUrl, getBlobSasUri, createBlobClient, createAccountSas } = await import('@simalicrum/azure-helpers'); 
    if (directory === "source" || directory === "documentation" || directory === "results") {
        const blobClient = createBlobClient(fileUrl, cromwellAccountKey); 
        filePath = blobClient.name; // full path relative to container 
        fileName = path.basename(filePath); // node.js path package 

        const destinationPath = "AX" + axId + "/" + directory + "/" + fileName; 
        const destinationUrl = createUrl(destinationPath, atlasAccount, atlasStagingContainer); 
        const destinationBlobClient = createBlobClient(destinationUrl, atlasAccountKey);  
        const sourceUrl = getBlobSasUri(filePath, sourceContainer, cromwellAccount, cromwellAccountKey, sourceContainerStoredPolicy); 

        const copied = await destinationBlobClient.beginCopyFromURL(sourceUrl); 
        const operationState = copied.getOperationState(); 
        const properties = await operationState.blobClient.getProperties(); 
        if (properties._response.status === 200 && (properties.copyStatus === "pending" || properties.copyStatus === "success")) {     
            context.log("COPYING to Atlas Staging (AX"  + axId + "): " + fileName); 
            fileUrlArray.push(destinationUrl); 
            return fileUrlArray; 
        } else {
            throw new Error('Copy process of ' + fileName + ' to Atlas Staging failed. Aborting cleanup!'); 
        }
    } else { 
        return null; 
    }
}

const copyToOutputStorage = async (fileUrl, sourceContainer, sourceContainerStoredPolicy, patientId, libraryId, context, study) => {
    const { createUrl, getBlobSasUri, createBlobClient, createAccountSas } = await import('@simalicrum/azure-helpers');
    const blobClient = createBlobClient(fileUrl, cromwellAccountKey); 
    filePath = blobClient.name; // full path relative to container 
    fileName = path.basename(filePath); // node.js path package 

    const destinationUrl = createUrl(patientId + "/" + libraryId + "/" + fileName, cromwellOutputsAccount, study); 
    const destinationBlobClient = createBlobClient(destinationUrl, cromwellOutputsAccountKey); 
    const sourceUrl = getBlobSasUri(filePath, sourceContainer, cromwellAccount, cromwellAccountKey, sourceContainerStoredPolicy); 

    const copied = await destinationBlobClient.beginCopyFromURL(sourceUrl); 
    const operationState = copied.getOperationState(); 
    const properties = await operationState.blobClient.getProperties(); 
    if (properties._response.status === 200 && (properties.copyStatus === "pending" || properties.copyStatus === "success")) {     
        context.log("COPYING to Outputs ("  + libraryId + "): " + fileName); 
    } else {
        throw new Error('Copy process of ' + fileName + ' to output storage failed. Aborting cleanup!'); 
    }
}

// if libraryId is included, then it will copy to output storage, otherwise it won't and will only do Atlas. 
// takes in path to file and copies file to Atlas (and local output storage if libraryId included)
const resultsToOutputsAndAtlas = async (inputPath, axId, context, fileUrlArray, patientId, libraryId, study) => {
    const { createUrl } = await import('@simalicrum/azure-helpers');
    // had to mess w/ string b/c azure blob parser couldn't handle the extra slash (///) when finding container from url
    const filePath = inputPath.substring(inputPath.indexOf('/', 1)).substring(inputPath.indexOf('/') + 1); 
    const sourceContainer = inputPath.split("/", 2)[1]; 
    const url = createUrl(filePath, cromwellAccount, sourceContainer); 
    fileUrlArray = await copyToAtlasStaging(url, axId, "results", sourceContainer, storedPolicyName, context, fileUrlArray); 
    if (libraryId) {
        const fileToLocal = await copyToOutputStorage(url, sourceContainer, storedPolicyName, patientId, libraryId, context, study); 
    }
    return fileUrlArray; 

    fileUrlArray = await resultsToOutputsAndAtlas(rawFlagstatPath, axId, context, fileUrlArray); 
}

// gets metadata.json object to parse through 
const getMetadata = async (bindingData) => {
    const { createUrl, readBlob } = await import('@simalicrum/azure-helpers');
    const metadataUrl = createUrl(bindingData.filename + "." + bindingData.workflowid + ".metadata.json", cromwellAccount, outputsContainer); 
    const metadataJson = await readBlob(metadataUrl, cromwellAccountKey); 
    const metadataObject = JSON.parse(metadataJson); 
    return metadataObject; 
}

// gets outputs.json object to parse through 
const getOutputs = async (bindingData) => {
    const { createUrl, readBlob } = await import('@simalicrum/azure-helpers');
    const outputsUrl = createUrl(bindingData.filename + "." + bindingData.workflowid + ".outputs.json", cromwellAccount, outputsContainer); 
    const outputsJson = await readBlob(outputsUrl, cromwellAccountKey); 
    const outputsObject = JSON.parse(outputsJson); 
    return outputsObject; 
}

const cleanSourceGeneral = async (axId, inputsUrl, optionsUrl, wdlUrl, context, fileUrlArray) => {
    const { deleteBlob } = await import('@simalicrum/azure-helpers'); 
    if (!Array.isArray(axId)) {
        axId = [axId]; 
    }
    
    // inputs.json
    for (let ax of axId) {
        const inputsToAtlas = await copyToAtlasStaging(inputsUrl, ax, "source", inputsContainer, storedPolicyName, context, fileUrlArray); 
    }
    
    // .wdl 
    for (let ax of axId) {
        const wdlToAtlas = await copyToAtlasStaging(wdlUrl, ax, "source", workflowTemplatesContainer, storedPolicyName, context, fileUrlArray); 
    }

    // options.json 
    if (optionsUrl) {
        const deleteOptions = await deleteBlob(optionsUrl, cromwellAccountKey, { deleteSnapshots: "include"}); 
        context.log("DELETING: " + context.bindingData.filename + "." + context.bindingData.workflowid + ".options.json"); 
    }

    return fileUrlArray; 
}

const cleanDocumentationGeneral = async (axId, directoryName, context, fileUrlArray) => {
    const { deleteBlob, createUrl, copyBlob } = await import('@simalicrum/azure-helpers'); 
    if (!Array.isArray(axId)) {
        axId = [axId]; 
    }

    const bindingData = context.bindingData; 
    const workflowName = bindingData.filename + "." + bindingData.workflowid; 
    
    // copy + delete metadata.json 
    const metadataName = workflowName + ".metadata.json"; 
    const metadataUrl = createUrl(metadataName, cromwellAccount, outputsContainer); 
    const metadataDestination = createUrl(directoryName + "/" + metadataName, cromwellAccount, outputsContainer); 
    const metadataToDatedFolder = await copyBlob(metadataUrl, metadataDestination, cromwellAccountKey); 
    if (metadataToDatedFolder.errorCode) { 
        throw new Error('Copy process failed. Aborting cleanup!'); 
    }
    for (let ax of axId) {
        const metadataToAtlas = await copyToAtlasStaging(metadataUrl, ax, "documentation", outputsContainer, storedPolicyName, context, fileUrlArray); 
    }

    // delete outputs.json 
    const outputsName = workflowName + ".outputs.json"; 
    const outputsUrl = createUrl(outputsName, cromwellAccount, outputsContainer); 
    const deleteOutputs = await deleteBlob(outputsUrl, cromwellAccountKey, { deleteSnapshots: "include"}); 

    // delete timing.html 
    const timingName = workflowName + ".timing.html"; 
    const timingUrl = createUrl(timingName, cromwellAccount, outputsContainer); 
    const deleteTiming = await deleteBlob(timingUrl, cromwellAccountKey, { deleteSnapshots: "include"}); 

    // copy + delete workflow.log
    const logName = "workflow." + bindingData.workflowid + ".log"; 
    const logUrl = createUrl(logName, cromwellAccount, workflowLogsContainer); 
    const logDestination = createUrl(directoryName + "/" + logName, cromwellAccount, workflowLogsContainer); 
    const logToDatedFolder = await copyBlob(logUrl, logDestination, cromwellAccountKey); 
    if (logToDatedFolder.errorCode) { 
        throw new Error('Copy process failed. Aborting cleanup!'); 
    }
    for (let ax of axId) {
        const logToAtlas = await copyToAtlasStaging(logUrl, ax, "documentation", workflowLogsContainer, storedPolicyName, context, fileUrlArray); 
    }

    // copy + delete trigger.json 
    const triggerName = workflowName + ".json"; 
    const triggerUrl = createUrl("succeeded/" + triggerName, cromwellAccount, workflowsContainer); 
    for (let ax of axId) {
        const triggerToAtlas = await copyToAtlasStaging(triggerUrl, ax, "documentation", workflowsContainer, storedPolicyName, context, fileUrlArray); 
    }

    return fileUrlArray
}

const cleanGeneral = async (axId, context, fileUrlArray, inputsUrl, optionsUrl, wdlUrl) => {  
    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    
    // Get dates for storage folder 
    const directoryName = await getFolderDate(inputsUrl); 
    // context.log("The dated directory name: " + directoryName); 
    
    // ------- SOURCE CLEANUP --------
    fileUrlArray = await cleanSourceGeneral(axId, inputsUrl, optionsUrl, wdlUrl, context, fileUrlArray); 


    // ------- DOCUMENTATION CLEANUP --------
    fileUrlArray = await cleanDocumentationGeneral(axId, directoryName, context, fileUrlArray); 

    // ------- RETURN ARRAY ------------
    return fileUrlArray; 
}


const cleanCleanPDX = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const { copyBlob, createUrl, createBlobClient } = await import('@simalicrum/azure-helpers'); 

    // Get Library ID (for inputs directory): 
    const libraryId = inputsObject["CleanPDX.sample_ID"]; 
    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 

    // Copy bam file: 
    // Did not push the bam destination URL to the fileUrlArray because it is not copying into the 
    // Atlas storage account. This should be fine, as the BAMs are being copied within the same storage account
    // and thus, should be quick. There may be edge cases, so please keep this in mind. 
    const bam = metadataObject.calls["CleanPDX.Reheader"][0].outputs.output_bam; 
    let filePath = bam.substring(bam.indexOf('/', 1)).substring(bam.indexOf('/') + 1); 
    fileName = path.basename(filePath); 
    const destinationUrl = createUrl(libraryId + "/" + fileName, cromwellAccount, inputsContainer); 
    const destinationBlobClient = createBlobClient(destinationUrl, cromwellAccountKey); 
    const sourceUrl = createUrl(filePath, cromwellAccount, executionsContainer); 
    const copied = await destinationBlobClient.beginCopyFromURL(sourceUrl); 
    const operationState = copied.getOperationState(); 
    const properties = await operationState.blobClient.getProperties(); 
    if (properties._response.status === 200 && (properties.copyStatus === "pending" || properties.copyStatus === "success")) { 
        context.log("COPYING TO INPUTS: " + fileName); 
    } else {
        throw new Error('Copy process of BAM to inputs failed. Aborting cleanup!'); 
    }

    // raw_flagstat to atlas 
    const rawFlagstatPath = metadataObject.calls["CleanPDX.Flagstat"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(rawFlagstatPath, axId, context, fileUrlArray); 

    // bam-header.txt to atlas 
    const bamHeaderPath = metadataObject.calls["CleanPDX.GetBamHeader"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(bamHeaderPath, axId, context, fileUrlArray);

    return fileUrlArray; 
}

const cleanCustomPrePro = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const libraryId = inputsObject["PreProcessing.sample_and_unmapped_bams"]["final_gvcf_base_name"]; 
    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(axId, token, atlasApiUrl, context); 

    // bam to outputs + atlas 
    const bamPath = metadataObject.calls["PreProcessing.UnmappedBamToAlignedBam"][0].outputs.output_bam; 
    fileUrlArray = await resultsToOutputsAndAtlas(bamPath, axId, context, fileUrlArray, patientId, libraryId, study); 

    // bai to outputs + atlas
    const baiPath = metadataObject.calls["PreProcessing.UnmappedBamToAlignedBam"][0].outputs.output_bam_index; 
    fileUrlArray = await resultsToOutputsAndAtlas(baiPath, axId, context, fileUrlArray, patientId, libraryId, study); 

    // flagstat.txt to outputs + atlas 
    const flagstatPath = metadataObject.calls["PreProcessing.Flagstat"][0].outputs.output_file;
    fileUrlArray = await resultsToOutputsAndAtlas(flagstatPath, axId, context, fileUrlArray, patientId, libraryId, study); 

    // wgs_metrics to outputs + atlas 
    const wgsPath = metadataObject.calls["PreProcessing.CollectWgsMetrics"][0].outputs.metrics;
    fileUrlArray = await resultsToOutputsAndAtlas(wgsPath, axId, context, fileUrlArray, patientId, libraryId, study); 

    // if the BamToCram exists, then proceed with copying
    if (metadataObject.calls["PreProcessing.BamToCram"][0].outputs.output_cram) {
        // cram to atlas
        const cramPath = metadataObject.calls["PreProcessing.BamToCram"][0].outputs.output_cram; 
        fileUrlArray = await resultsToOutputsAndAtlas(cramPath, axId, context, fileUrlArray); 
        // cram.crai to atlas
        const craiPath = metadataObject.calls["PreProcessing.BamToCram"][0].outputs.output_cram_index; 
        fileUrlArray = await resultsToOutputsAndAtlas(craiPath, axId, context, fileUrlArray); 
        // cram.md5 to atlas
        const md5Path = metadataObject.calls["PreProcessing.BamToCram"][0].outputs.output_cram_md5; 
        fileUrlArray = await resultsToOutputsAndAtlas(md5Path, axId, context, fileUrlArray); 
    }

    // unmapped.quality_yield_metrics to atlas 
    const metricsArray = metadataObject.calls["PreProcessing.UnmappedBamToAlignedBam"][0].outputs.quality_yield_metrics; 
    for (let metricsPath of metricsArray) {
        fileUrlArray = await resultsToOutputsAndAtlas(metricsPath, axId, context, fileUrlArray); 
    }
    
    // duplicate_metrics to atlas
    const duplicateMetricsPath = metadataObject.calls["PreProcessing.UnmappedBamToAlignedBam"][0].outputs.duplicate_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(duplicateMetricsPath, axId, context, fileUrlArray); 

    // recal_data.csv to atlas 
    const recalDataPath = metadataObject.calls["PreProcessing.UnmappedBamToAlignedBam"][0].outputs.output_bqsr_reports; 
    fileUrlArray = await resultsToOutputsAndAtlas(recalDataPath, axId, context, fileUrlArray); 

    // bam-header.txt to atlas 
    const bamHeaderPath = metadataObject.calls["PreProcessing.GetBamHeader"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(bamHeaderPath, axId, context, fileUrlArray);

    return fileUrlArray; 
}

const cleanUbamPrePro = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {  
    const libraryId = inputsObject["UbamPrePro.sample_info"]["base_file_name"]; 
    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(axId, token, atlasApiUrl, context); 

    // flagstat to outputs + atlas 
    const flagstatPath = metadataObject.calls["UbamPrePro.Flagstat"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(flagstatPath, axId, context, fileUrlArray, patientId, libraryId, study); 

    // wgs metrics to outputs + atlas 
    const wgsPath = metadataObject.calls["UbamPrePro.CollectWgsMetrics"][0].outputs.metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(wgsPath, axId, context, fileUrlArray, patientId, libraryId, study);

    // bam-header.txt to atlas 
    const bamHeaderPath = metadataObject.calls["UbamPrePro.GetBamHeader"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(bamHeaderPath, axId, context, fileUrlArray);

    // metrics to atlas 
    const metricsArray = metadataObject.calls["UbamPrePro.UnmappedBamToAlignedBam"][0].outputs.quality_yield_metrics; 
    for (let metricsPath of metricsArray) {
        fileUrlArray = await resultsToOutputsAndAtlas(metricsPath, axId, context, fileUrlArray); 
    }
    
    // duplicate metrics to atlas 
    const duplicateMetricsPath = metadataObject.calls["UbamPrePro.UnmappedBamToAlignedBam"][0].outputs.duplicate_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(duplicateMetricsPath, axId, context, fileUrlArray); 

    // recal data to atlas 
    const recalDataPath = metadataObject.calls["UbamPrePro.UnmappedBamToAlignedBam"][0].outputs.output_bqsr_reports; 
    fileUrlArray = await resultsToOutputsAndAtlas(recalDataPath, axId, context, fileUrlArray);  

    // bai to outputs + atlas 
    const baiPath = metadataObject.calls["UbamPrePro.UnmappedBamToAlignedBam"][0].outputs.output_bam_index; 
    fileUrlArray = await resultsToOutputsAndAtlas(baiPath, axId, context, fileUrlArray, patientId, libraryId, study); 
    
    // bam to outputs + atlas 
    // const bamPath = metadataObject.calls["UbamPrePro.UnmappedBamToAlignedBam"][0].outputs.output_bam; 
    // fileUrlArray = await resultsToOutputsAndAtlas(bamPath, axId, context, fileUrlArray, patientId, libraryId, study); 

    if (metadataObject.calls["UbamPrePro.BamToCram"][0].outputs.output_cram) {
        // cram to atlas 
        const cramPath = metadataObject.calls["UbamPrePro.BamToCram"][0].outputs.output_cram; 
        fileUrlArray = await resultsToOutputsAndAtlas(cramPath, axId, context, fileUrlArray); 

        // crai to atlas 
        const craiPath = metadataObject.calls["UbamPrePro.BamToCram"][0].outputs.output_cram_index; 
        fileUrlArray = await resultsToOutputsAndAtlas(craiPath, axId, context, fileUrlArray); 

        // md5 to atlas 
        const md5Path = metadataObject.calls["UbamPrePro.BamToCram"][0].outputs.output_cram_md5; 
        fileUrlArray = await resultsToOutputsAndAtlas(md5Path, axId, context, fileUrlArray); 
    }

    return fileUrlArray; 
}

const cleanUbamGermPrePro = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const libraryId = inputsObject["UbamGermlinePrePro.sample_info"]["base_file_name"]; 
    const preProAx = axId[0]; 
    const htcAx = axId[1]; 

    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(preProAx, token, atlasApiUrl, context); 

    // bam to outputs + atlas 
    const bamPath = metadataObject.calls["UbamGermlinePrePro.UnmappedBamToAlignedBam"][0].outputs.output_bam; 
    fileUrlArray = await resultsToOutputsAndAtlas(bamPath, preProAx, context, fileUrlArray, patientId, libraryId, study); 

    // bai to outputs + atlas 
    const baiPath = metadataObject.calls["UbamGermlinePrePro.UnmappedBamToAlignedBam"][0].outputs.output_bam_index; 
    fileUrlArray = await resultsToOutputsAndAtlas(baiPath, preProAx, context, fileUrlArray, patientId, libraryId, study); 

    // flagstat to outputs + atlas 
    const flagstatPath = metadataObject.calls["UbamGermlinePrePro.Flagstat"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(flagstatPath, preProAx, context, fileUrlArray, patientId, libraryId, study); 

    if (metadataObject.calls["UbamGermlinePrePro.BamToCram"][0].outputs.output_cram) {
        // cram to atlas
        const cramPath = metadataObject.calls["UbamGermlinePrePro.BamToCram"][0].outputs.output_cram; 
        fileUrlArray = await resultsToOutputsAndAtlas(cramPath, preProAx, context, fileUrlArray); 

        // crai to atlas
        const craiPath = metadataObject.calls["UbamGermlinePrePro.BamToCram"][0].outputs.output_cram_index; 
        fileUrlArray = await resultsToOutputsAndAtlas(craiPath, preProAx, context, fileUrlArray); 

        // md5 to atlas 
        const md5Path = metadataObject.calls["UbamGermlinePrePro.BamToCram"][0].outputs.output_cram_md5; 
        fileUrlArray = await resultsToOutputsAndAtlas(md5Path, preProAx, context, fileUrlArray); 
    }

    // metrics to atlas 
    const metricsArray = metadataObject.calls["UbamGermlinePrePro.UnmappedBamToAlignedBam"][0].outputs.quality_yield_metrics; 
    for (let metricsPath of metricsArray) {
        fileUrlArray = await resultsToOutputsAndAtlas(metricsPath, preProAx, context, fileUrlArray); 
    }
    
    // duplicate metrics to atlas 
    const duplicateMetricsPath = metadataObject.calls["UbamGermlinePrePro.UnmappedBamToAlignedBam"][0].outputs.duplicate_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(duplicateMetricsPath, preProAx, context, fileUrlArray); 

    // recal data to atlas 
    const recalDataPath = metadataObject.calls["UbamGermlinePrePro.UnmappedBamToAlignedBam"][0].outputs.output_bqsr_reports;
    fileUrlArray = await resultsToOutputsAndAtlas(recalDataPath, preProAx, context, fileUrlArray); 

    // bam-header.txt to atlas 
    const bamHeaderPath = metadataObject.calls["UbamGermlinePrePro.GetBamHeader"][0].outputs.output_file; 
    fileUrlArray = await resultsToOutputsAndAtlas(bamHeaderPath, axId, context, fileUrlArray);

    // clean haplotype caller workflow
    fileUrlArray = await cleanHtc(htcAx, libraryId, patientId, metadataObject, context, fileUrlArray, study); 

    return fileUrlArray; 
}

const cleanHtc = async (htcAx, libraryId, patientId, metadataObject, context, fileUrlArray, study) => {
    // alignment summary to atlas 
    const alignmentSummaryPath = metadataObject.calls["UbamGermlinePrePro.AggregatedBamQC"][0].outputs.agg_alignment_summary_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(alignmentSummaryPath, htcAx, context, fileUrlArray); 

    // gc bias to atlas 
    const gcBiasPath = metadataObject.calls["UbamGermlinePrePro.AggregatedBamQC"][0].outputs.agg_gc_bias_detail_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(gcBiasPath, htcAx, context, fileUrlArray); 

    // raw wgs metrics to atlas 
    const rawWgsMetricsPath = metadataObject.calls["UbamGermlinePrePro.CollectRawWgsMetrics"][0].outputs.metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(rawWgsMetricsPath, htcAx, context, fileUrlArray); 

    // read group alignment to atlas 
    const readgroupAlignmentPath = metadataObject.calls["UbamGermlinePrePro.AggregatedBamQC"][0].outputs.read_group_alignment_summary_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(readgroupAlignmentPath, htcAx, context, fileUrlArray); 

    // read group gc bias to atlas 
    const readgroupGcBiasPath = metadataObject.calls["UbamGermlinePrePro.AggregatedBamQC"][0].outputs.read_group_gc_bias_detail_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(readgroupGcBiasPath, htcAx, context, fileUrlArray); 

    // variant calling to atlas 
    const variantCallingDetailPath = metadataObject.calls["UbamGermlinePrePro.BamToGvcf"][0].outputs.vcf_detail_metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(variantCallingDetailPath, htcAx, context, fileUrlArray); 

    // variant calling summary path to atlas 
    const variantCallingSummaryPath = metadataObject.calls["UbamGermlinePrePro.BamToGvcf"][0].outputs.vcf_summary_metrics;
    fileUrlArray = await resultsToOutputsAndAtlas(variantCallingSummaryPath, htcAx, context, fileUrlArray); 

    // vcf to outputs + atlas 
    const vcfPath = metadataObject.calls["UbamGermlinePrePro.BamToGvcf"][0].outputs.output_vcf; 
    fileUrlArray = await resultsToOutputsAndAtlas(vcfPath, htcAx, context, fileUrlArray, patientId, libraryId, study); 

    // vcf.idx to outputs + atlas 
    const vcfIndexPath = metadataObject.calls["UbamGermlinePrePro.BamToGvcf"][0].outputs.output_vcf_index; 
    fileUrlArray = await resultsToOutputsAndAtlas(vcfIndexPath, htcAx, context, fileUrlArray, patientId, libraryId, study); 

    // wgs to outputs + atlas 
    const wgsPath = metadataObject.calls["UbamGermlinePrePro.CollectWgsMetrics"][0].outputs.metrics; 
    fileUrlArray = await resultsToOutputsAndAtlas(wgsPath, htcAx, context, fileUrlArray, patientId, libraryId, study); 

    return fileUrlArray; 
}

const cleanMutect2Cbio = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const normalId = inputsObject["Mutect2.normal_id"]; 
    const tumorId = inputsObject["Mutect2.tumor_id"]; 
    const combinedId = tumorId + "-" + normalId + "-mutect2"; 
    const mutectAx = axId[0]; 
    const cbioAx = axId[1]; 

    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(mutectAx, token, atlasApiUrl, context); 

    // filtered vcf to outputs + atlas 
    const filteredVcfPath = outputsObject.outputs["Mutect2.filtered_vcf"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(filteredVcfPath, mutectAx, context, fileUrlArray, patientId, combinedId, study); 

    // filtered vcf.idx to outputs + atlas 
    const filteredVcfIndexPath = outputsObject.outputs["Mutect2.filtered_vcf_idx"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(filteredVcfIndexPath, mutectAx, context, fileUrlArray, patientId, combinedId, study); 

    // maf funcotated index to outputs + atlas 
    const mafFuncotatedIndexPath = outputsObject.outputs["Mutect2.maf_funcotated_file_index"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(mafFuncotatedIndexPath, mutectAx, context, fileUrlArray, patientId, combinedId, study); 
    
    // filtering_stats to atlas 
    const filteringStatsPath = outputsObject.outputs["Mutect2.filtering_stats"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(filteringStatsPath, mutectAx, context, fileUrlArray); 

    // mutect_stats to atlas 
    const mergedStatsPath = outputsObject.outputs["Mutect2.mutect_stats"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(mergedStatsPath, mutectAx, context, fileUrlArray); 

    // funcotated file to atlas 
    const funcotatedFilePath = outputsObject.outputs["Mutect2.funcotated_file"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(funcotatedFilePath, mutectAx, context, fileUrlArray); 

    // funcotated index to atlas 
    const funcotatedIndexPath = outputsObject.outputs["Mutect2.funcotated_file_index"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(funcotatedIndexPath, mutectAx, context, fileUrlArray); 

    // maf funcotated to atlas 
    const mafFuncotatedPath = outputsObject.outputs["Mutect2.maf_funcotated_file"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(mafFuncotatedPath, mutectAx, context, fileUrlArray);

    // clean cbio
    fileUrlArray = await cleanCbio(cbioAx, combinedId, patientId, outputsObject, context, fileUrlArray, study); 

    return fileUrlArray; 
}

const cleanCbio = async (cbioAx, combinedId, patientId, outputsObject, context, fileUrlArray, study) => {
    const minMafPath = outputsObject.outputs["Mutect2.cbio_maf"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(minMafPath, cbioAx, context, fileUrlArray, patientId, combinedId, study); 

    return fileUrlArray; 
}

// shard order corresponds to order of sample IDs in inputs.json, which corresponds to the order of AXIDs made. 
const cleanReadCounterIchor = async (axIdArray, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {    
    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 

    let shardCounter = 0; 
    const normalId = inputsObject["ReadCounterIchor.rc_normal_inputs"]["normal_id"]; 

    // loop through the axIdArray, copying respective files to each axId (axId order + shard order are both based on the inputs.json order, so they will be the same!)
    for (let axId of axIdArray) {
        const patientId = await getPatientId(axId, token, atlasApiUrl, context);
        const tumorId = inputsObject["ReadCounterIchor.rc_tumor_inputs"][shardCounter]["left"]; 
        const combinedId = tumorId + "-" + normalId + "-ichor"; 

        // params.txt file to outputs + atlas 
        const paramsPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.params; 
        fileUrlArray = await resultsToOutputsAndAtlas(paramsPath, axId, context, fileUrlArray, patientId, combinedId, study); 

        // RData file to outputs + atlas 
        const rDataPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.rdata; 
        fileUrlArray = await resultsToOutputsAndAtlas(rDataPath, axId, context, fileUrlArray, patientId, combinedId, study); 

        // wig file to atlas
        const tumorWigPath = metadataObject.calls["ReadCounterIchor.tumorRc"][shardCounter].outputs.output_wig; 
        fileUrlArray = await resultsToOutputsAndAtlas(tumorWigPath, axId, context, fileUrlArray); 

        // only copy the normal .wig file to the first atlas analysis object: 
        if (shardCounter === 0) {
            const normalWigPath = metadataObject.calls["ReadCounterIchor.normalRc"][0].outputs.output_wig; 
            fileUrlArray = await resultsToOutputsAndAtlas(normalWigPath, axId, context, fileUrlArray); 
        }

        // cna.seg file to atlas
        const cnaSegPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.cna;
        fileUrlArray = await resultsToOutputsAndAtlas(cnaSegPath, axId, context, fileUrlArray); 

        // seg file to atlas
        const segPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.seg;
        fileUrlArray = await resultsToOutputsAndAtlas(segPath, axId, context, fileUrlArray); 

        // seg.txt file to atlas
        const segTxtPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.segTxt;
        fileUrlArray = await resultsToOutputsAndAtlas(segTxtPath, axId, context, fileUrlArray);

        // corrected_depth file to atlas
        const correctedDepthPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.corrected_depth;
        fileUrlArray = await resultsToOutputsAndAtlas(correctedDepthPath, axId, context, fileUrlArray);

        // correct.pdf file to atlas
        const correctPdfPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.correct;
        fileUrlArray = await resultsToOutputsAndAtlas(correctPdfPath, axId, context, fileUrlArray);

        // genomeWide.pdf file to atlas
        const genomeWidePath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.genome_wide;
        fileUrlArray = await resultsToOutputsAndAtlas(genomeWidePath, axId, context, fileUrlArray);

        // genomeWide_all_sols.pdf file to atlas
        const genomeWideAllSolsPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.genome_wide_all_sols;
        fileUrlArray = await resultsToOutputsAndAtlas(genomeWideAllSolsPath, axId, context, fileUrlArray);

        // tpdf.pdf file to atlas
        const tpdfPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.tpdf;
        fileUrlArray = await resultsToOutputsAndAtlas(tpdfPath, axId, context, fileUrlArray);

        // bias.pdf file to atlas
        const biasPath = metadataObject.calls["ReadCounterIchor.ichorCNA"][shardCounter].outputs.bias;
        fileUrlArray = await resultsToOutputsAndAtlas(biasPath, axId, context, fileUrlArray);

        shardCounter++; 
    }
    return fileUrlArray
}

const cleanSequenzaWorkflow = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const normalId = inputsObject["SequenzaWorkflow.normalName"]; 
    const tumorId = inputsObject["SequenzaWorkflow.tumorName"]; 
    const combinedId = tumorId + "-" + normalId + "-sequenza";

    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(axId, token, atlasApiUrl, context); 

    
    // sequenza.tar.gz to atlas 
    const scnaResPath = outputsObject.outputs["SequenzaWorkflow.SequenzaTask.scnaRes"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(scnaResPath, axId, context, fileUrlArray); 

    // seqz_bin.tar.gz to atlas 
    const binSeqzPath = outputsObject.outputs["SequenzaWorkflow.SequenzaTask.binSeqz"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(binSeqzPath, axId, context, fileUrlArray); 

    // logs.tar.gz to atlas 
    const logsPath = outputsObject.outputs["SequenzaWorkflow.SequenzaTask.logs"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(logsPath, axId, context, fileUrlArray); 

    return fileUrlArray; 
}

const cleanWaspMapping = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const normalId = inputsObject["WaspMapping.normal_name"]; 
    const tumorId = inputsObject["WaspMapping.sample_name"]; 
    const combinedId = tumorId + "-" + normalId + "-wasp";

    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(axId, token, atlasApiUrl, context); 

    
    // snp_index to outputs + atlas
    const snpIndexPath = outputsObject.outputs["WaspMapping.snp_index"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(snpIndexPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // haplotype to outputs + atlas
    const haplotypePath = outputsObject.outputs["WaspMapping.haplotype"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(haplotypePath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // snp_tab to outputs + atlas
    const snpTabPath = outputsObject.outputs["WaspMapping.snp_tab"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(snpTabPath, axId, context, fileUrlArray, patientId, combinedId, study); 
    
    // sorted bam to outputs + atlas
    const sortedBamPath = outputsObject.outputs["WaspMapping.sorted_bam"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(sortedBamPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // sorted bai to outputs + atlas
    const sortedBaiPath = outputsObject.outputs["WaspMapping.sorted_bam_index"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(sortedBaiPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    return fileUrlArray; 
}

const cleanPairedSVCaller = async (axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study) => {
    const normalId = inputsObject["PairedSVCaller.normal_name"]; 
    const tumorId = inputsObject["PairedSVCaller.sample_name"]; 
    const combinedId = tumorId + "-" + normalId + "-paired-sv-caller";

    const bindingData = context.bindingData; 
    const metadataObject = await getMetadata(bindingData); 
    const outputsObject = await getOutputs(bindingData); 
    const patientId = await getPatientId(axId, token, atlasApiUrl, context); 

    
    // manta_filtered to outputs + atlas
    const mantaFiltPath = outputsObject.outputs["PairedSVCaller.manta_filtered"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(mantaFiltPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // manta_unfiltered to outputs + atlas
    const mantaUnfiltPath = outputsObject.outputs["PairedSVCaller.manta_unfiltered"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(mantaUnfiltPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // lumpy_filtered to outputs + atlas
    const lumpyFiltPath = outputsObject.outputs["PairedSVCaller.lumpy_filtered"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(lumpyFiltPath , axId, context, fileUrlArray, patientId, combinedId, study); 
    
    // lumpy_unfiltered to outputs + atlas
    const lumpyUnfiltPath = outputsObject.outputs["PairedSVCaller.lumpy_unfiltered"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(lumpyUnfiltPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // gridss_filtered to outputs + atlas
    const gridssFiltPath = outputsObject.outputs["PairedSVCaller.gridss_filtered"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(gridssFiltPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // gridss_unfiltered to outputs + atlas
    const gridssUnfiltPath = outputsObject.outputs["PairedSVCaller.gridss_unfiltered"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(gridssUnfiltPath, axId, context, fileUrlArray, patientId, combinedId, study); 

    // merged1 to outputs + atlas
    const merged1Path = outputsObject.outputs["PairedSVCaller.merged1"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(merged1Path, axId, context, fileUrlArray, patientId, combinedId, study); 

    // merged2 to outputs + atlas
    const merged2Path = outputsObject.outputs["PairedSVCaller.merged2"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(merged2Path, axId, context, fileUrlArray, patientId, combinedId, study); 

    // merged3 to outputs + atlas
    const merged3Path = outputsObject.outputs["PairedSVCaller.merged3"]; 
    fileUrlArray = await resultsToOutputsAndAtlas(merged3Path, axId, context, fileUrlArray, patientId, combinedId, study);

    return fileUrlArray; 
}

module.exports = { cleanCleanPDX, cleanCustomPrePro, cleanUbamPrePro, cleanUbamGermPrePro, cleanHtc, cleanMutect2Cbio, cleanCbio, cleanGeneral, cleanReadCounterIchor, cleanSequenzaWorkflow, cleanWaspMapping, cleanPairedSVCaller };








