"""
User Management Page Module
Complete CRUD operations for users (Admin only)
"""

import streamlit as st
import pandas as pd

def show_user_management(controllers, permissions):
    """
    Display user management page with full CRUD.
    Admin access only.

    Args:
        repos: Dictionary of repository instances
        services: Dictionary of service instances
        permissions: User permissions dictionary
    """
    st.markdown('<div class="main-header">👥 User Management</div>', unsafe_allow_html=True)

    if not permissions.get('can_manage_users'):
        st.warning("⚠️ Administrator access required")
        st.info("📧 Contact your system administrator for access")
        return

    tabs = ["📋 View Users", "➕ Create User", "✏️ Update User", "🗑️ Delete User", "🏢 Departments"]
    tab_objects = st.tabs(tabs)

    # ========== TAB 1: VIEW USERS ==========
    with tab_objects[0]:
        st.markdown("### 📋 All Users")

        try:
            users = controllers['user'].get_all_users()

            if users:
                df = pd.DataFrame(users)

                # Remove password hash from display
                if 'password_hash' in df.columns:
                    df = df.drop('password_hash', axis=1)

                st.caption(f"Total Users: {len(df)}")

                # Display table
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "is_active": st.column_config.CheckboxColumn("Active"),
                        "created_date": st.column_config.DatetimeColumn(
                            "Created",
                            format="YYYY-MM-DD HH:mm"
                        ),
                        "last_login": st.column_config.DatetimeColumn(
                            "Last Login",
                            format="YYYY-MM-DD HH:mm"
                        ),
                        "days_since_last_login": st.column_config.NumberColumn(
                            "Days Inactive",
                            format="%d"
                        )
                    }
                )

                # User statistics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Users", len(df))

                with col2:
                    active_count = len(df[df['is_active'] == True])
                    st.metric("Active Users", active_count)

                with col3:
                    if 'role_name' in df.columns:
                        admin_count = len(df[df['role_name'] == 'Admin'])
                        st.metric("Admins", admin_count)

                with col4:
                    if 'days_since_last_login' in df.columns:
                        inactive = len(df[df['days_since_last_login'] > 30])
                        st.metric("Inactive (>30d)", inactive)

            else:
                st.info("No users found")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 2: CREATE USER ==========
    with tab_objects[1]:
        st.markdown("### ➕ Create New User")

        with st.form("create_user_form", clear_on_submit=True):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Account Information**")

                username = st.text_input(
                    "Username *",
                    max_chars=50,
                    placeholder="johndoe",
                    help="Unique username (3-50 characters)"
                )

                password = st.text_input(
                    "Password *",
                    type="password",
                    help="Minimum 6 characters"
                )

                confirm_password = st.text_input(
                    "Confirm Password *",
                    type="password"
                )

                email = st.text_input(
                    "Email *",
                    placeholder="user@equity.com",
                    help="Valid email address"
                )

            with col2:
                st.markdown("**User Details**")

                full_name = st.text_input(
                    "Full Name *",
                    placeholder="John Doe"
                )

                # Get roles
                try:

                    roles = controllers['user'].get_all_roles()


                    if roles:
                        role_dict = {
                            f"{r['role_name']} ({r['permission_level']})": r['role_id']
                            for r in roles
                        }
                        selected_role = st.selectbox("Role *", list(role_dict.keys()))
                        role_id = role_dict[selected_role]
                    else:
                        st.error("No roles available")
                        role_id = None
                except Exception as e:
                    print(e)
                    st.error("Error loading roles")
                    role_id = None

                # Get departments
                try:
                    departments = controllers['user'].get_all_departments()

                    if departments:
                        dept_dict = {d['department_name']: d['department_id'] for d in departments}
                        dept_dict['None'] = None
                        selected_dept = st.selectbox("Department", list(dept_dict.keys()))
                        department_id = dept_dict[selected_dept]
                    else:
                        department_id = None
                except:
                    department_id = None

                phone_number = st.text_input(
                    "Phone Number",
                    placeholder="+1-555-0100"
                )

            submitted = st.form_submit_button("✅ Create User", use_container_width=True, type="primary")

            if submitted:
                # Validation
                errors = []

                if not username or len(username) < 3:
                    errors.append("Username must be at least 3 characters")

                if not password or len(password) < 6:
                    errors.append("Password must be at least 6 characters")

                if password != confirm_password:
                    errors.append("Passwords do not match")

                if not email or '@' not in email:
                    errors.append("Valid email is required")

                if not full_name:
                    errors.append("Full name is required")

                if role_id is None:
                    errors.append("Role must be selected")

                if errors:
                    for error in errors:
                        st.error(f"❌ {error}")
                else:
                    try:
                        controllers['user'].create_user(
                            username=username,
                            email=email,
                            password=password,
                            full_name=full_name,
                            role_id=role_id,
                            department_id=department_id,
                            phone_number=phone_number if phone_number else None
                        )

                        st.success(f"✅ User '{username}' created successfully!")
                        st.balloons()
                        st.info(f"💡 Login credentials: {username} / {password}")

                    except Exception as e:
                        st.error(f"❌ Error creating user: {e}")

    # ========== TAB 3: UPDATE USER ==========
    with tab_objects[2]:
        st.markdown("### ✏️ Update User")

        try:
            users = controllers['user'].get_all_users()

            if users:
                user_dict = {
                    f"{u['username']} - {u['full_name']} ({u['role_name']})": u['user_id']
                    for u in users
                }

                selected = st.selectbox("Select User to Update", list(user_dict.keys()))
                user_id = user_dict[selected]

                # Load user
                user_data = controllers['user'].get_user_by_id(user_id)

                if user_data:
                    user = user_data

                    with st.form("update_user_form"):
                        col1, col2 = st.columns(2)

                        with col1:
                            new_full_name = st.text_input("Username", value=user['username'])
                            new_email = st.text_input("Email", value=user['email'])

                        with col2:
                            new_phone = st.text_input("Phone", value=user.get('phone_number', ''))

                            is_active = st.checkbox("Active", value=bool(user['is_active']))

                        submitted = st.form_submit_button("💾 Update User", use_container_width=True, type="primary")

                        if submitted:
                            try:
                                controllers['user'].update_user(user_id,
                                    new_full_name, new_email, new_phone, is_active
                                )


                                st.success("✅ User updated successfully!")
                                st.balloons()

                            except Exception as e:
                                st.error(f"❌ Error: {e}")
            else:
                st.info("No users available")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 4: DELETE USER ==========
    with tab_objects[3]:
        st.markdown("### 🗑️ Delete User")

        st.warning("⚠️ Warning: This will permanently delete the user account!")

        try:
            users = controllers['user'].get_all_users()

            if users:
                user_dict = {
                    f"{u['username']} - {u['full_name']}": u['user_id']
                    for u in users
                }


                selected = st.selectbox("Select User to Delete", list(user_dict.keys()))
                user_id = user_dict[selected]

                confirm = st.checkbox(f"I confirm I want to delete user: {selected}")

                if st.button("🗑️ Delete User", type="primary", disabled=not confirm):
                    try:
                        controllers['user'].delete_user(user_id, True)
                        st.success("✅ User deleted successfully!")

                    except Exception as e:
                        st.error(f"❌ Error: {e}")
            else:
                st.info("No users available")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    # ========== TAB 5: DEPARTMENTS ==========
    with tab_objects[4]:
        st.markdown("### 🏢 Department Management")

        department_most_active = controllers['user'].get_active_dept()

        st.markdown(f"Most active department '{department_most_active}'")

        col1, col2 = st.columns(2)


        with col1:
            st.markdown("#### 📋 All Departments")

            try:
                departments = controllers['user'].get_all_departments()

                if departments:
                    df = pd.DataFrame(departments)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No departments found")
            except Exception as e:
                st.error(f"Error: {e}")

        with col2:
            st.markdown("#### ➕ Add Department")

            with st.form("add_department_form", clear_on_submit=True):
                dept_name = st.text_input("Department Name *")
                dept_desc = st.text_area("Description", height=100)

                submitted = st.form_submit_button("Create Department", use_container_width=True)

                if submitted:
                    if not dept_name:
                        st.error("Department name is required")
                    else:
                        try:
                            controllers['department'].create(dept_name, dept_desc)
                            st.success(f"✅ Department '{dept_name}' created!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")