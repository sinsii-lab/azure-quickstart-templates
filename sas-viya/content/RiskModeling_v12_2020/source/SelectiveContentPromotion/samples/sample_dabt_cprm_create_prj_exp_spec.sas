%csbmva_initialize_cr(m_cr_unique_cd=RM_CONTENT);
%rmcr_init_cprm;
%let project_id_lst=4,5,7;
%dabt_cprm_create_prj_exp_spec
(project_id_lst = %quote(&project_id_lst));
