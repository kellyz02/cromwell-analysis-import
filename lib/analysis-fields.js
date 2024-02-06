const { refConfig } = require("../config/reference"); 
const { i_ref } = refConfig; 

// type of reference shouldn't matter for CleanPDX 
const analysisCleanPDX = (date, researcher, relatedInputs, ref) => {
    return {
        d_submission_date: date, 
        s_analysis_type: "Mouse Read Filtering", 
        s_researcher: researcher, 
        i_reference_file: i_ref,                       // reference file inputs in .config file
        s_reference_file: ref,
        s_additional_requirements: "No results files kept, since the outputs of CleanPDX are only used as inputs to the pre-processing workflow.\nReference chimeric mm10/hg38 genome: coaf3b65459446be3/workflow-templates/clean-pdx/refs/mouse_human_genome.fa", 
        fk_experiments: relatedInputs
    }
 }

const analysisCustomPrePro = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Pre-processing", 
            s_analysis_subtype: "GATK Pre-processing Pipeline", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Reference hg38 Genome: /coaf3b65459446be3/inputs/pre-pro-refs/Homo_sapiens_assembly38.fasta",
            fk_analyses: relatedInputs
        }
    }
 }

 const analysisUbamPrePro = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Pre-processing", 
            s_analysis_subtype: "GATK Pre-processing Pipeline", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Reference hg38 Genome: /coaf3b65459446be3/inputs/pre-pro-refs/Homo_sapiens_assembly38.fasta",
            fk_experiments: relatedInputs
        }
    }
 }

const analysisUbamGermPrePro = (date, researcher, relatedInputs, ref) => {  
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Pre-processing", 
            s_analysis_subtype: "GATK Pre-processing Pipeline", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Reference hg38 Genome: /coaf3b65459446be3/inputs/pre-pro-refs/Homo_sapiens_assembly38.fasta",
            fk_experiments: relatedInputs
        }
    }
}

const analysisHtc = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Germline Short Variant Calling", 
            s_analysis_subtype: "GATK HaplotypeCaller", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Reference hg38 Genome: /coaf3b65459446be3/inputs/pre-pro-refs/Homo_sapiens_assembly38.fasta",
            fk_analyses: relatedInputs,  
        }
    }
}

const analysisMutect2 = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Somatic Short Variant Calling", 
            s_analysis_subtype: "GATK Mutect2 Tumour-Normal Paired", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Funcotator References: funcotator_dataSources.20230215s.tar.gz", // needs to be updated if funcotator is
            fk_analyses: relatedInputs,  
        }
    }
}

const analysisCbio = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "MAF Annotation for cBioPortal", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "MafFormatter run as a sub-workflow in Mutect2. Source code: https://github.com/jliebe-bccrc/cromwell-workflows/tree/main/cbio-workflows/maf-formatter", 
            fk_analyses: relatedInputs,  
        }
    }
}

const analysisReadCounterIchor = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Copy Number Analysis", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "ichorCNA v0.2.0, https://github.com/broadinstitute/ichorCNA", 
            fk_analyses: relatedInputs,  
        }
    }
}

const analysisSequenzaWorkflow = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Copy Number Analysis", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Sequenza v3.0.0, https://sequenzatools.bitbucket.io/#/home \nReference hg38 Genome: /coaf3b65459446be3/inputs/sequenza-refs/Homo_sapiens_assembly38.fasta.gz", 
            fk_analyses: relatedInputs
        }
    }
}

const analysisWaspMapping = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Copy Number Analysis", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "Wasp Mapping, https://github.com/bmvdgeijn/WASP \nReference hg38 Genome: /coaf3b65459446be3/inputs/pre-pro-refs/Homo_sapiens_assembly38.fasta",
            fk_analyses: relatedInputs
        }
    }
}

const analysisPairedSVCaller = (date, researcher, relatedInputs, ref) => {
    if (ref === "hg38") {
        return {
            d_submission_date: date, 
            s_analysis_type: "Long Variant Calling", 
            s_analysis_subtype: "SURVIVOR", 
            s_researcher: researcher, 
            i_reference_file: i_ref, 
            s_reference_file: ref,
            s_additional_requirements: "https://github.com/aparicio-bioinformatics-coop/cromwell-workflows/blob/main/sv-caller/PairedSVCaller.wdl \nReference hg38 Genome: /coaf3b65459446be3/inputs/pre-pro-refs/Homo_sapiens_assembly38.fasta",
            fk_analyses: relatedInputs
        }
    }
}

const analysisFields = { // keys should match string elements extracted from inputs.json file 
    "CleanPDX": analysisCleanPDX, 
    "PreProcessing": analysisCustomPrePro,
    "UbamPrePro": analysisUbamPrePro, 
    "UbamGermlinePrePro": analysisUbamGermPrePro, 
    "Htc": analysisHtc, 
    "Mutect2": analysisMutect2, 
    "Cbio": analysisCbio, 
    "ReadCounterIchor": analysisReadCounterIchor,
    "SequenzaWorkflow": analysisSequenzaWorkflow,
    "WaspMapping": analysisWaspMapping,
    "PairedSVCaller": analysisPairedSVCaller
} 


module.exports = { analysisFields };


