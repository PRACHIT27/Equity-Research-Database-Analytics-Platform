"""
User Repository Module
Data access layer for User and Department tables
ALL SQL queries and stored procedure calls here
"""

from typing import List, Optional, Dict, Any
from repositories.BaseRepository import BaseRepository
import bcrypt

class UserRepository(BaseRepository):
    """Repository for user operations"""

    def find_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        pass

    def get_table_name(self) -> str:
        return "User"

    # ========== CREATE ==========

    def create(self, username: str, email: str, password: str, full_name: str,
               role_id: int, department_id: Optional[int] = None,
               phone_number: Optional[str] = None) -> int:
        """Create new user with hashed password"""
        cost_factor = 12
        prefix_version = b'2a' # Must be a bytes object

        # Generate the salt with the specified cost and version
        # The 'rounds' parameter is the cost factor (logarithmic work factor)
        custom_salt = bcrypt.gensalt(rounds=cost_factor, prefix=prefix_version)

        # Hash the password using the custom salt
        password_hash = bcrypt.hashpw(password.encode('utf-8'), custom_salt).decode('utf-8')

        query = """
                INSERT INTO Users (
                    username, email, password_hash, full_name, role_id,
                    department_id, phone_number, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE) \
                """

        return self.execute_custom_update(query, (
            username, email, password_hash, full_name, role_id,
            department_id, phone_number
        ))

    # ========== READ ==========

    def find_by_id_full(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Find user by ID with full data (including password_hash)"""

        query = "SELECT * FROM Users WHERE user_id = %s"
        results = self.execute_custom_query(query, (user_id,))
        return results[0] if results else None

    def find_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Find user by username"""

        query = "SELECT * FROM Users WHERE username = %s"
        results = self.execute_custom_query(query, (username,))
        return results[0] if results else None

    def find_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Find user by email"""

        query = "SELECT * FROM Users WHERE email = %s"
        results = self.execute_custom_query(query, (email,))
        return results[0] if results else None

    def find_role_by_id(self, role_id: int) -> Optional[Dict[str, Any]]:
        """Find role by ID"""

        query = """
                SELECT r.*, p.*
                FROM Role r
                         INNER JOIN Permission p ON r.permission_level = p.permission_level
                WHERE r.role_id = %s \
                """

        results = self.execute_custom_query(query, (role_id,))
        return results[0] if results else None

    def get_all_roles(self) -> Optional[Dict[str, Any]]:
        """Find role by ID"""

        query = """
                SELECT r.*, p.*
                FROM Role r
                         INNER JOIN Permission p ON r.permission_level = p.permission_level
                ORDER BY r.role_id DESC \
                """

        results = self.execute_custom_query(query)
        return results if results else None

    def find_all_with_details(self) -> List[Dict[str, Any]]:
        """Get all users with role and department info"""

        query = """
                SELECT
                    u.user_id,
                    u.username,
                    u.email,
                    u.full_name,
                    r.role_name,
                    d.department_name,
                    u.phone_number,
                    u.created_date,
                    u.last_login,
                    u.days_since_last_login,
                    u.is_active
                FROM Users u
                         INNER JOIN Role r ON u.role_id = r.role_id
                         LEFT JOIN Department d ON u.department_id = d.department_id
                ORDER BY u.created_date DESC \
                """

        return self.execute_custom_query(query)

    # ========== STORED PROCEDURE: Authentication ==========

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate user and return user data with permissions.
        Uses stored procedure: AuthenticateUser
        """
        try:
            # First get user to verify password
            user = self.find_by_username(username)

            if not user:
                return None

            if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
                return None

            # Call stored procedure to get full user details with permissions
            password_hash = user['password_hash']
            results = self.call_stored_procedure('AuthenticateUser', (username, password_hash))

            return results[0] if results else None

        except Exception as e:
            print(f"Authentication error: {e}")
            return None

    # ========== UPDATE ==========

    def update_by_id(self, user_id: int, **kwargs) -> int:
        """Update user by ID with any fields"""

        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE Users SET {set_clause} WHERE user_id = %s"
        params = tuple(data.values()) + (user_id,)

        return self.execute_custom_update(query, params)

    def update_last_login(self, user_id: int) -> int:
        """Update user's last login timestamp"""

        query = """
                UPDATE Users
                SET last_login = CURRENT_TIMESTAMP,
                    days_since_last_login = 0
                WHERE user_id = %s \
                """

        return self.execute_custom_update(query, (user_id,))

    def update_password(self, user_id: int, new_password: str) -> int:
        """Update user password (hashes automatically)"""

        # Hash new password
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        query = "UPDATE Users SET password_hash = %s WHERE user_id = %s"
        return self.execute_custom_update(query, (password_hash, user_id))

    def activate_user(self, user_id: int) -> int:
        """Activate user account"""

        query = "UPDATE Users SET is_active = TRUE WHERE user_id = %s"
        return self.execute_custom_update(query, (user_id,))

    def deactivate_user(self, user_id: int) -> int:
        """Deactivate user account"""

        query = "UPDATE Users SET is_active = FALSE WHERE user_id = %s"
        return self.execute_custom_update(query, (user_id,))

    # ========== DELETE ==========

    def delete_by_id(self, user_id: int) -> int:
        """Delete user by ID"""

        query = "DELETE FROM Users WHERE user_id = %s"
        return self.execute_custom_update(query, (user_id,))

    # ========== USER-DEFINED FUNCTIONS ==========

    def has_permission(self, user_id: int, permission_type: str) -> bool:
        """
        Check if user has permission using database function.
        Uses: HasPermission(user_id, permission_type)
        """
        try:
            result = self.call_function('HasPermission', (user_id, permission_type))
            return bool(result)
        except:
            return False

    def get_permission_level(self, user_id: int) -> int:
        """
        Get user's permission level using database function.
        Uses: GetUserPermissionLevel(user_id)
        """
        try:
            result = self.call_function('GetUserPermissionLevel', (user_id,))
            return int(result) if result else 0
        except:
            return 0

    # ========== ANALYTICS ==========

    def get_user_activity_summary(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user activity summary"""

        query = """
                SELECT
                    u.username,
                    u.full_name,
                    u.last_login,
                    u.days_since_last_login,
                    COUNT(ua.action_id) as total_actions,
                    SUM(CASE WHEN ua.action_type = 'Create' THEN 1 ELSE 0 END) as create_count,
                    SUM(CASE WHEN ua.action_type = 'Update' THEN 1 ELSE 0 END) as update_count,
                    SUM(CASE WHEN ua.action_type = 'Delete' THEN 1 ELSE 0 END) as delete_count
                FROM Users u
                         LEFT JOIN User_Action ua ON u.user_id = ua.user_id
                WHERE u.user_id = %s
                GROUP BY u.user_id, u.username, u.full_name, u.last_login, u.days_since_last_login \
                """

        results = self.execute_custom_query(query, (user_id,))
        return results[0] if results else None


class DepartmentRepository(BaseRepository):
    """Repository for department operations"""

    def get_table_name(self) -> str:
        return "Department"

    def create(self, department_name: str, description: Optional[str] = None) -> int:
        """Create new department"""

        query = """
                INSERT INTO Department (department_name, description, is_active)
                VALUES (%s, %s, TRUE) \
                """

        return self.execute_custom_update(query, (department_name, description))

    def find_by_id(self, department_id: int) -> Optional[Dict[str, Any]]:
        """Find department by ID"""

        query = "SELECT * FROM Department WHERE department_id = %s"
        results = self.execute_custom_query(query, (department_id,))
        return results[0] if results else None

    def find_by_name(self, department_name: str) -> Optional[Dict[str, Any]]:
        """Find department by name"""

        query = "SELECT * FROM Department WHERE department_name = %s"
        results = self.execute_custom_query(query, (department_name,))
        return results[0] if results else None

    def find_all(self) -> List[Dict[str, Any]]:
        """Get all departments"""

        query = """
                SELECT
                    department_id,
                    department_name,
                    description,
                    is_active,
                    created_at
                FROM Department
                ORDER BY department_name \
                """

        return self.execute_custom_query(query)

    def get_with_user_counts(self) -> List[Dict[str, Any]]:
        """Get all departments with user counts"""

        query = """
                SELECT
                    d.department_id,
                    d.department_name,
                    COUNT(u.user_id) as user_count
                FROM Department d
                         LEFT JOIN Users u ON d.department_id = u.department_id
                GROUP BY d.department_id, d.department_name
                ORDER BY d.department_name \
                """

        return self.execute_custom_query(query)

    def update_by_id(self, department_id: int, **kwargs) -> int:
        """Update department"""

        data = {k: v for k, v in kwargs.items() if v is not None}

        if not data:
            return 0

        return self.update('department_id', department_id, data)

    def delete_by_id(self, department_id: int) -> int:
        """Delete department (users will have dept_id set to NULL)"""

        query = "DELETE FROM Department WHERE department_id = %s"
        return self.execute_custom_update(query, (department_id,))

    def get_dept_count(self) -> str:
        """
        Check if user has permission using database function.
        Uses: HasPermission(user_id, permission_type)
        """
        try:
            result = self.call_function('GetMostActiveDepartment')
            return result
        except:
            return "No dept is active"